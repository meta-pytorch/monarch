/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <Python.h>
#include <algorithm>
#include <cstddef>
#include <iostream>
#include <memory>
#include <type_traits>
#include <vector>

#include <ATen/SparseCsrTensorUtils.h> // @manual=//caffe2:torch_extension
#include <c10/util/flat_hash_map.h> // @manual=//caffe2:torch_extension
#include <torch/csrc/autograd/edge.h> // @manual=//caffe2:torch_extension
#include <torch/csrc/autograd/function.h> // @manual=//caffe2:torch_extension
#include <torch/csrc/autograd/input_buffer.h> // @manual=//caffe2:torch_extension
#include <torch/csrc/autograd/python_cpp_function.h> // @manual=//caffe2:torch_extension
#include <torch/csrc/autograd/python_function.h> // @manual=//caffe2:torch_extension
#include <torch/extension.h> // @manual=//caffe2:torch_extension

#define TORCH_VERSION_NEWER_THAN(major, minor, patch)                   \
  ((TORCH_VERSION_MAJOR > (major)) ||                                   \
   (TORCH_VERSION_MAJOR == (major) && TORCH_VERSION_MINOR > (minor)) || \
   (TORCH_VERSION_MAJOR == (major) && TORCH_VERSION_MINOR == (minor) && \
    TORCH_VERSION_PATCH > (patch)))

using torch::autograd::Edge;
using torch::autograd::InputBuffer;
using torch::autograd::Node;
using torch::autograd::THPCppFunction;
using torch::autograd::THPCppFunction_Check;

using ContextRestorer = std::function<
    std::function<void()>(const at::Tensor&, std::optional<size_t>, bool)>;

struct ContextGuard {
  explicit ContextGuard(std::function<void()> exit) : exit_(std::move(exit)) {}
  ~ContextGuard() {
    exit_();
  }

 private:
  std::function<void()> exit_;
};

static inline bool shouldDebugDump() {
  static bool debugEnabled = []() {
    const char* envVar = std::getenv("TORCH_MONARCH_GRAD_GENERATOR_DUMP");
    return envVar != nullptr && std::string(envVar) == "1";
  }();
  return debugEnabled;
}

#define DEBUG_PRINT(...)        \
  do {                          \
    if (shouldDebugDump()) {    \
      std::cout << __VA_ARGS__; \
    }                           \
  } while (0)

template <typename T>
struct Slice {
  T* begin() const {
    return begin_;
  }
  T* end() const {
    return end_;
  }
  size_t size() const {
    return end_ - begin_;
  }
  T& operator[](size_t i) const {
    TORCH_CHECK(begin_ + i < end_, "index out of range");
    return begin_[i];
  }
  T* begin_;
  T* end_;
};
struct Arena {
  Arena()
      : next_free_(inline_buffer_), remaining_size_(sizeof(inline_buffer_)) {}
  template <typename T, typename... Args>
  T* allocate(Args&&... args) {
    static_assert(std::is_pod<T>::value, "T must be a POD type.");
    return allocate_slice<T>(1, std::forward<Args>(args)...).begin();
  }
  template <typename T, typename... Args>
  Slice<T> allocate_slice(size_t n, Args&&... args) {
    static_assert(std::is_pod<T>::value, "T must be a POD type.");
    size_t alignment = alignof(T);
    size_t bytes_needed = n * sizeof(T);
    size_t to_align = 0;
    size_t align_remainder =
        reinterpret_cast<uintptr_t>(next_free_) % alignment;
    if (align_remainder > 0) {
      to_align = alignment - align_remainder;
    }
    if (to_align + bytes_needed > remaining_size_) {
      allocate_new_buffer(std::max(bytes_needed, sizeof(inline_buffer_)));
    }
    next_free_ += to_align;
    T* result = reinterpret_cast<T*>(next_free_);
    for (auto i : c10::irange(bytes_needed)) {
      next_free_[i] = 24;
    }
    next_free_ += bytes_needed;
    remaining_size_ -= (to_align + bytes_needed);
    auto slice = Slice<T>{result, result + n};
    for (auto& r : slice) {
      r.init(std::forward<Args>(args)...);
    }
    return slice;
  }
  ~Arena() {
    for (char* buffer : buffers_) {
      free(buffer);
    }
  }

 private:
  void allocate_new_buffer(size_t min_size) {
    size_t size = std::max(min_size, sizeof(inline_buffer_));
    char* new_buffer = static_cast<char*>(malloc(size));
    buffers_.push_back(new_buffer);
    next_free_ = new_buffer;
    remaining_size_ = size;
  }
  char inline_buffer_[1024];
  std::vector<char*> buffers_; // use malloc/free
  char* next_free_;
  size_t remaining_size_;
};

// POD types we put in the area
// each has an init since CTORs are not allowed.

struct NodeState;
struct Use {
  NodeState* user;
  size_t offset;
  Use* next;
  void init() {
    user = nullptr;
    offset = 0;
    next = nullptr;
  }
};

constexpr int NOT_NEEDED = -1;

struct InputBufferReference {
  void init() {
    first_user = nullptr;
    needed = NOT_NEEDED;
    result_index = NOT_NEEDED;
  }
  Use* first_user;
  int result_index; // if this buffer was requested,
                    // or NOT_NEEDED
  int needed;
};

InputBufferReference not_used = {nullptr, NOT_NEEDED, NOT_NEEDED};

struct EdgeState {
  NodeState* node_state;
  size_t offset;
  Use use;
  InputBufferReference& input_buffer();
  void init() {
    node_state = nullptr;
    offset = 0;
    use.init();
  }
};

struct NodeState {
  void
  init(Arena& arena, std::vector<InputBuffer>& all_input_buffers, Node* n) {
    node = n;
    input_buffers = arena.allocate_slice<InputBufferReference>(n->num_inputs());
    next = arena.allocate_slice<EdgeState>(n->num_outputs());
    input_buffers_offset = all_input_buffers.size();
    all_input_buffers.emplace_back(input_buffers.size());
    needed = NOT_NEEDED;
    last_stage = NOT_NEEDED;
    users_remaining = 0;
  }
  void setNext(size_t i, NodeState* n, size_t offset) {
    InputBufferReference& ib = n->input_buffers[offset];
    Use* next_use = ib.first_user;
    EdgeState& es = next[i];
    es.node_state = n;
    es.offset = offset;
    es.use = Use{this, i, next_use};
    ib.first_user = &es.use;
    n->users_remaining++;
  }
  void init_viz() {
    DEBUG_PRINT(int64_t(this) << " [label=\"");
    for (auto i : c10::irange(input_buffers.size())) {
      DEBUG_PRINT("<I" << i << "> S" << input_buffers[i].needed << " |");
    }
    DEBUG_PRINT(node->name() << " S" << needed);
    for (auto i : c10::irange(next.size())) {
      DEBUG_PRINT("| <O" << i << ">S" << next[i].input_buffer().needed);
    }
    DEBUG_PRINT("\"]\n");
  }
  Node* node;
  Slice<InputBufferReference> input_buffers;
  Slice<EdgeState> next;
  int needed;
  int last_stage;
  size_t input_buffers_offset;
  size_t users_remaining;
};

InputBufferReference& EdgeState::input_buffer() {
  return node_state == nullptr ? not_used : node_state->input_buffers[offset];
}

struct Root {
  Edge reference;
  std::optional<at::Tensor> grad_root;
};

struct CompareNode {
  bool operator()(
      const std::pair<int, NodeState*>& a,
      const std::pair<int, NodeState*>& b) {
    if (a.first > b.first) {
      return true;
    }
    return a.second->node->sequence_nr() < b.second->node->sequence_nr();
  }
};

std::function<void()> emptyContext(
    const at::Tensor& t,
    std::optional<size_t> sequence_nr,
    bool last) {
  return []() {};
}

// ported from engine.cpp:validate_outputs
at::Tensor check_and_reduce(Node* node, uint32_t input_nr, at::Tensor grad) {
  if (!node || !grad.defined()) {
    return grad;
  }
  const auto& metadata = node->input_metadata(input_nr);
  // first argument is an index used for debugging that
  // this code doesn't yet thread through, so use a constant
  // that hopefully will let someone know that it is fake
  grad = metadata.maybe_reduce(
      123456, std::move(grad), [&](const std::string& x) { return x; });

  bool input_is_complex =
      isComplexType(c10::typeMetaToScalarType(metadata.options().dtype()));
  bool grad_is_complex = isComplexType(grad.scalar_type());

  TORCH_CHECK(
      isFloatingType(grad.scalar_type()) ||
      (input_is_complex == grad_is_complex));
  if (c10::typeMetaToScalarType(metadata.options().dtype()) !=
      grad.scalar_type()) {
    grad = grad.to(c10::typeMetaToScalarType(metadata.options().dtype()));
  }
  if (grad.dtype() != metadata.dtype()) {
    std::stringstream ss;
    ss << "invalid gradient at index - expected dtype ";
    ss << metadata.dtype() << " but got " << grad.dtype();
    TORCH_CHECK(false, ss.str());
  }
  if (grad.layout() != metadata.layout()) {
    // TODO: Currently we only support (*, Sparse) combination for
    // (tensor.layout(), tensor.grad.layout()) In future, there will be an
    // opportunity to support more combinations of layouts if they are
    // composable (example., operations like addition etc., are well defined
    // between tensors of different layouts.), as well as all parts of
    // autograd like AccumulateGrad correctly handle this. We allow grad to be
    // Strided when metadata is SparseCsr
    if (!grad.is_sparse() &&
        !(grad.layout() == at::kStrided &&
          (at::sparse_csr::is_sparse_compressed(metadata.layout()) ||
           metadata.layout() == at::kSparse))) {
      std::stringstream ss;
      ss << "invalid gradient - expected layout ";
      ss << metadata.layout() << " but got " << grad.layout();
      TORCH_CHECK(false, ss.str());
    }
  }

  if (grad.device() != metadata.device()) {
    // quick hack for: https://github.com/pytorch/pytorch/issues/65016 but
    // should be eventually removed
    if (!(metadata.is_tensor_subclass() ||
          grad.unsafeGetTensorImpl()->is_python_dispatch())) {
      if (grad.dim() == 0) {
        grad = grad.to(metadata.device());
      } else {
        std::stringstream ss;
        ss << "invalid gradient  - expected device ";
        ss << metadata.device() << " but got " << grad.device();
        TORCH_CHECK(false, ss.str());
      }
    }
  }
  // We should not build graph for Tensors that are not differentiable
  TORCH_INTERNAL_ASSERT(
      torch::autograd::isDifferentiableType(grad.scalar_type()));
  return grad;
}

struct GradientGenerator {
  GradientGenerator(
      std::vector<Root> roots = {},
      std::vector<Edge> with_respect_to = {},
      ContextRestorer cr = emptyContext)
      : roots_(std::move(roots)),
        with_respect_to_(with_respect_to),
        context_restorer_(std::move(cr)) {
    buildGraph();
  }

  GradientGenerator& iter() {
    return *this;
  }
  bool next(std::optional<at::Tensor>& value) {
    while (true) {
      DEBUG_PRINT("// current stage: " << currentStage() << "\n");
      if (next_buffer_ < currentStage()) {
        value = std::move(results_.at(next_buffer_));
        DEBUG_PRINT(" // yielding: " << next_buffer_ << "\n");
        next_buffer_++;
        return true;
      }

      if (ready_heap_.empty()) {
        return false;
      }
      auto stage = ready_heap_.top().first;
      auto ready = ready_heap_.top().second;
      ready_heap_.pop();
      run(stage, ready);
      for (auto& output : ready->next) {
        auto needed_stage = output.input_buffer().needed;
        if (needed_stage != stage) {
          continue;
        }
        output.node_state->users_remaining--;
        if (output.node_state->users_remaining == 0) {
          addReady(output.node_state);
        }
      }
    }
  }

 private:
  void run(int stage, NodeState* ready) {
    Node* node = ready->node;
    DEBUG_PRINT("// running " << node->name() << " at stage " << stage << "\n");
    c10::SmallVector<Edge> to_restore;
    for (auto i : c10::irange(ready->next.size())) {
      auto& output = ready->next[i];
      auto needed_stage = output.input_buffer().needed;
      if (needed_stage != stage) {
        to_restore.emplace_back(std::move(node->next_edges().at(i)));
      } else {
        to_restore.emplace_back();
      }
    }
    // no need for example tensor because the only case we do not have a
    // sequence nr is accumulate grad nodes. accumulate grad nodes do not ever
    // get run.
    auto guard =
        restoreContext(ready, at::Tensor(), stage == ready->last_stage);
    auto& input_buffer = realInputBuffer(ready);
    std::vector<at::Tensor> inputs = (stage == ready->last_stage)
        ? std::move(input_buffer.buffer)
        : input_buffer.buffer;
    std::vector<at::Tensor> outputs = (*node)(std::move(inputs));
    if (stage == ready->last_stage) {
      DEBUG_PRINT("// last stage, releasing variables\n");
      node->release_variables();
    }
    for (auto i : c10::irange(outputs.size())) {
      if (ready->next[i].input_buffer().needed == stage) {
        auto output = ready->next[i];
        add(output.node_state, output.offset, std::move(outputs.at(i)));
      } else {
        node->next_edges().at(i) = std::move(to_restore.at(i));
      }
    }
  }

  std::pair<bool, NodeState*> getOrCreateState(Node* node) {
    auto it = node_state_.find(node);
    if (it != node_state_.end()) {
      return std::make_pair(false, &(*it->second));
    } else {
      NodeState* state =
          arena_.allocate<NodeState>(arena_, all_input_buffers_, node);
      node_state_.emplace(node, state);
      return std::make_pair(true, state);
    }
  }

  ContextGuard
  restoreContext(NodeState* node, const at::Tensor& example, bool last) {
    std::optional<size_t> sequence_nr = node->node->sequence_nr();
    if (sequence_nr == UINT64_MAX) {
      // node is AccumulateGrad. Normally we can figure out the appropriate
      // context from the tensor being added. The only exception is
      sequence_nr = std::nullopt;
    }
    return ContextGuard(context_restorer_(example, sequence_nr, last));
  }

  void add(NodeState* node, size_t input_nr, at::Tensor t) {
    DEBUG_PRINT(
        "// add: " << node->node->name()
                   << ", input_nr=" << static_cast<int>(input_nr) << "\n");
#if TORCH_VERSION_NEWER_THAN(2, 9, 1)
    realInputBuffer(node).add(
        input_nr,
        check_and_reduce(node->node, input_nr, std::move(t)),
        std::nullopt,
        std::nullopt,
        node->node);
#else
    realInputBuffer(node).add(
        input_nr,
        check_and_reduce(node->node, input_nr, std::move(t)),
        std::nullopt,
        std::nullopt);
#endif
  }

  InputBuffer& realInputBuffer(NodeState* state) {
    return all_input_buffers_.at(state->input_buffers_offset);
  }

  void buildGraph() {
    DEBUG_PRINT("digraph G {\nnode [shape=record];\n");
    std::vector<NodeState*> worklist;
    results_.resize(with_respect_to_.size());
    size_t root_i = 0;
    for (auto& root : roots_) {
      auto r = getOrCreateState(root.reference.function.get());
      DEBUG_PRINT(
          int64_t(&root) << " [label=\"root " << root_i++ << " "
                         << (root.grad_root ? "with grad" : "no grad")
                         << "\"]\n");
      DEBUG_PRINT(
          int64_t(&root) << " -> " << int64_t(r.second) << ":I"
                         << root.reference.input_nr << "\n");
      if (r.first) {
        worklist.push_back(r.second);
      }
      if (root.grad_root) {
        ContextGuard guard = restoreContext(r.second, *root.grad_root, false);
        add(r.second, root.reference.input_nr, *root.grad_root);
      } else {
        // XXX - corner case: this is a grad accumulate node (no sequence
        // number) then there is no example tensor, and no sequence number to
        // look up what to restore. We will end up using the current
        // device_mesh/stream. Probably not important because it only happens in
        // a degenerate autograd call (root and with_respect_to are the same
        // tensor)
        ContextGuard guard = restoreContext(
            r.second,
            realInputBuffer(r.second).buffer.at(root.reference.input_nr),
            false);
        auto& md = r.second->node->input_metadata(root.reference.input_nr);
        add(r.second,
            root.reference.input_nr,
            torch::ones_symint(md.shape_as_dim_vector(), md.options()));
      }
    }
    while (!worklist.empty()) {
      NodeState* state = worklist.back();
      worklist.pop_back();
      for (auto i : c10::irange(state->node->num_outputs())) {
        const Edge& producer_edge = state->node->next_edge(i);
        if (!producer_edge.is_valid()) {
          continue;
        }
        auto producer_state = getOrCreateState(producer_edge.function.get());
        if (producer_state.first) {
          worklist.push_back(producer_state.second);
        }
        DEBUG_PRINT(
            int64_t(state) << ":O" << i << " -> "
                           << int64_t(producer_state.second) << ":I"
                           << producer_edge.input_nr << "\n");
        state->setNext(i, producer_state.second, producer_edge.input_nr);
      }
    }
    std::vector<NodeState*> all_ready;
    std::vector<NodeState*> scan;
    for (auto i : c10::irange(with_respect_to_.size())) {
      Edge& handle = with_respect_to_.at(i);
      DEBUG_PRINT(
          int64_t(&with_respect_to_[i])
          << " [label=\"with_respect_to " << i << "\"]\n");
      auto it = node_state_.find(handle.function.get());
      if (it == node_state_.end()) {
        // no path to this node, so gradient will be None
        continue;
      }
      NodeState* state = it->second;
      InputBufferReference& ib = state->input_buffers[handle.input_nr];
      ib.result_index = i;
      DEBUG_PRINT(
          int64_t(&with_respect_to_[i])
          << " -> " << int64_t(state) << ":I" << handle.input_nr << "\n");
      if (ib.needed != NOT_NEEDED) {
        continue;
      }
      ib.needed = i;
      Use* use = ib.first_user;
      while (use) {
        scan.push_back(use->user);
        use = use->next;
      }
      while (!scan.empty()) {
        NodeState* scan_state = scan.back();
        scan.pop_back();
        if (scan_state->needed != NOT_NEEDED) {
          continue;
        }
        scan_state->needed = i;
        if (scan_state->users_remaining == 0) {
          all_ready.push_back(scan_state);
        }
        for (InputBufferReference& ibuf : scan_state->input_buffers) {
          if (ibuf.needed != NOT_NEEDED) {
            continue;
          }
          ibuf.needed = i;
          use = ibuf.first_user;
          while (use) {
            scan.push_back(use->user);
            use = use->next;
          }
        }
      }
    }
    for (auto& ready : all_ready) {
      addReady(ready);
    }
    for (auto& it : node_state_) {
      it.second->init_viz();
    }
    DEBUG_PRINT("}}\n");
  }

  void addReady(NodeState* ready) {
    for (auto i : c10::irange(ready->input_buffers.size())) {
      auto result_index = ready->input_buffers[i].result_index;
      if (result_index != NOT_NEEDED) {
        auto& t = realInputBuffer(ready).buffer.at(i);
        results_.at(result_index) =
            (ready->needed == NOT_NEEDED) ? std::move(t) : t;
      }
    }
    c10::SmallVector<int, 8> stages;
    for (auto& output : ready->next) {
      auto ib = output.input_buffer();
      if (ib.needed != NOT_NEEDED &&
          std::find(stages.begin(), stages.end(), ib.needed) == stages.end()) {
        stages.push_back(ib.needed);
        ready->last_stage = std::max(ready->last_stage, ib.needed);
        ready_heap_.emplace(ib.needed, ready);
      }
    }
  }

  int currentStage() {
    if (ready_heap_.empty()) {
      return with_respect_to_.size();
    }
    return ready_heap_.top().first;
  }

  ska::flat_hash_map<Node*, NodeState*> node_state_;
  std::vector<Root> roots_;
  std::vector<Edge> with_respect_to_;
  std::vector<std::optional<at::Tensor>> results_;
  std::vector<InputBuffer> all_input_buffers_;
  std::priority_queue<
      std::pair<int, NodeState*>,
      std::vector<std::pair<int, NodeState*>>,
      CompareNode>
      ready_heap_;
  int next_buffer_ = 0;
  ContextRestorer context_restorer_;
  Arena arena_;
};

typedef struct {
  PyObject_HEAD GradientGenerator* obj;
} PyGradientGenerator;

static int convertNode(PyObject* obj, std::shared_ptr<Node>* node) {
  if (THPFunction_Check(obj)) {
    *node = ((THPFunction*)obj)->cdata.lock();
    return 1;
  } else if (THPCppFunction_Check(obj)) {
    *node = ((THPCppFunction*)obj)->cdata;
    return 1;
  } else {
    return 0;
  }
}

std::optional<Edge> parseEdge(PyObject* obj) {
  std::shared_ptr<Node> node;
  int input_nr;
  if (THPVariable_Check(obj)) {
    auto tensor = THPVariable_Unpack(obj);
    return torch::autograd::impl::gradient_edge(tensor);
  } else if (PyArg_ParseTuple(obj, "O&i", &convertNode, &node, &input_nr)) {
    return Edge(std::move(node), input_nr);
  }
  return std::nullopt;
}

static int PyGradientGenerator_init(
    PyGradientGenerator* self,
    PyObject* args,
    PyObject* kwds) {
  HANDLE_TH_ERRORS
  PyObject* roots_list = nullptr;
  PyObject* with_respect_to_list = nullptr;
  PyObject* grad_roots_list = nullptr;
  PyObject* context_restorer = nullptr;
  static const char* kwlist[] = {
      "roots", "with_respect_to", "grad_roots", "context_restorer", NULL};

  if (!PyArg_ParseTupleAndKeywords(
          args,
          kwds,
          "OO|OO",
          const_cast<char**>(kwlist),
          &roots_list,
          &with_respect_to_list,
          &grad_roots_list,
          &context_restorer)) {
    return -1;
  }
  std::vector<Root> roots;
  std::vector<Edge> with_respect_to;
  // Parse roots
  if (!PyList_Check(roots_list)) {
    PyErr_SetString(PyExc_TypeError, "roots must be a list");
    return -1;
  }
  Py_ssize_t num_roots = PyList_Size(roots_list);
  for (Py_ssize_t i = 0; i < num_roots; i++) {
    PyObject* item = PyList_GetItem(roots_list, i);
    auto edge = parseEdge(item);
    if (!edge) {
      PyErr_SetString(
          PyExc_TypeError,
          "Each item in roots must be a tuple (Node, int) or a Tensor");
      return -1;
    }
    roots.push_back({std::move(*edge), std::nullopt});
  }
  // Parse with_respect_to
  if (!PyList_Check(with_respect_to_list)) {
    PyErr_SetString(PyExc_TypeError, "with_respect_to must be a list");
    return -1;
  }
  Py_ssize_t num_edges = PyList_Size(with_respect_to_list);
  for (Py_ssize_t i = 0; i < num_edges; i++) {
    PyObject* item = PyList_GetItem(with_respect_to_list, i);
    auto edge = parseEdge(item);
    if (!edge) {
      PyErr_SetString(
          PyExc_TypeError,
          "Each item in with_respect_to must be a tuple (Node, int) or a Tensor");
      return -1;
    }
    with_respect_to.push_back(*edge);
  }
  // Optionally parse grad_roots if provided
  if (grad_roots_list) {
    if (!PyList_Check(grad_roots_list)) {
      PyErr_SetString(PyExc_TypeError, "grad_roots must be a list");
      return -1;
    }
    Py_ssize_t num_grad_roots = PyList_Size(grad_roots_list);
    if (num_grad_roots > num_roots) {
      PyErr_SetString(
          PyExc_TypeError,
          "grad_roots must be a list of tensors with the same length as roots");
      return -1;
    }
    for (Py_ssize_t i = 0; i < num_grad_roots; i++) {
      PyObject* tensor_obj = PyList_GetItem(grad_roots_list, i);
      if (!Py_IsNone(tensor_obj)) {
        if (!THPVariable_Check(tensor_obj)) {
          PyErr_SetString(
              PyExc_TypeError, "Each item in grad_roots must be a Tensor");
          return -1;
        }
        roots.at(i).grad_root = THPVariable_Unpack(tensor_obj);
      }
    }
  }
  ContextRestorer restore_context = emptyContext;
  if (context_restorer) {
    auto obj = py::reinterpret_borrow<py::object>(context_restorer);
    restore_context = [obj = std::move(obj)](
                          const at::Tensor& example,
                          std::optional<size_t> sequence_nr,
                          bool last) mutable {
      auto it = py::iter(obj(example, sequence_nr, last));
      it++;
      return [it = std::move(it)]() mutable { it++; };
    };
  }
  self->obj = new GradientGenerator(
      std::move(roots), std::move(with_respect_to), std::move(restore_context));
  return 0;
  END_HANDLE_TH_ERRORS_RET(-1);
}
static void PyGradientGenerator_dealloc(PyGradientGenerator* self) {
  delete self->obj;
  Py_TYPE(self)->tp_free((PyObject*)self);
}

static PyMethodDef PyGradientGenerator_methods[] = {
    {NULL} /* Sentinel */
};

static PyObject* PyGradientGenerator_iter(PyObject* self) {
  HANDLE_TH_ERRORS
  PyGradientGenerator* pyObj = reinterpret_cast<PyGradientGenerator*>(self);
  pyObj->obj->iter();
  Py_INCREF(self);
  return self;
  END_HANDLE_TH_ERRORS
}

static PyObject* PyGradientGenerator_iternext(PyObject* self) {
  HANDLE_TH_ERRORS
  PyGradientGenerator* pyObj = reinterpret_cast<PyGradientGenerator*>(self);
  std::optional<at::Tensor> value;
  if (pyObj->obj->next(value)) {
    // Assuming you have a function to convert at::Tensor to PyObject*
    if (value.has_value()) {
      return THPVariable_Wrap(*value);
    } else {
      Py_RETURN_NONE;
    }
  } else {
    // When no more items are available, raise StopIteration
    PyErr_SetNone(PyExc_StopIteration);
    return NULL;
  }
  END_HANDLE_TH_ERRORS
}

static PyTypeObject PyGradientGeneratorType = {PyVarObject_HEAD_INIT(NULL, 0)};

static PyModuleDef gradientmodule = {
    PyModuleDef_HEAD_INIT,
    "monarch.gradient._gradient_generator",
    "Python interface for the GradientGenerator C++ class",
    -1,
    NULL,
    NULL,
    NULL,
    NULL,
    NULL};

PyMODINIT_FUNC PyInit__gradient_generator(void) {
  PyGradientGeneratorType.tp_name =
      "monarch.gradient._gradient_generator.GradientGenerator";
  PyGradientGeneratorType.tp_basicsize = sizeof(PyGradientGenerator);
  PyGradientGeneratorType.tp_itemsize = 0;
  PyGradientGeneratorType.tp_dealloc = (destructor)PyGradientGenerator_dealloc;
  PyGradientGeneratorType.tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE;
  PyGradientGeneratorType.tp_doc = "GradientGenerator";
  PyGradientGeneratorType.tp_iter = PyGradientGenerator_iter;
  PyGradientGeneratorType.tp_iternext = PyGradientGenerator_iternext;
  PyGradientGeneratorType.tp_methods = PyGradientGenerator_methods;
  PyGradientGeneratorType.tp_init = (initproc)PyGradientGenerator_init;
  PyGradientGeneratorType.tp_new = PyType_GenericNew;

  PyObject* m;
  if (PyType_Ready(&PyGradientGeneratorType) < 0) {
    return NULL;
  }
  m = PyModule_Create(&gradientmodule);
  if (m == NULL) {
    return NULL;
  }
  Py_INCREF(&PyGradientGeneratorType);
  if (PyModule_AddObject(
          m, "GradientGenerator", (PyObject*)&PyGradientGeneratorType) < 0) {
    Py_DECREF(&PyGradientGeneratorType);
    Py_DECREF(m);
    return NULL;
  }
  return m;
}
