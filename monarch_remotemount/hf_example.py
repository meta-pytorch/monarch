import torch
from transformers import pipeline

# 1. Select a sub-10B model
# Microsoft Phi-3 Mini is ~3.8 Billion parameters
model_id = "microsoft/Phi-3-mini-4k-instruct"

print(f"Downloading and loading {model_id}...")

# 2. Initialize the pipeline
# device_map="auto" will automatically use your GPU if available, otherwise CPU
# torch_dtype=torch.float16 reduces memory usage by half (requires GPU usually)
pipe = pipeline(
    "text-generation",
    model=model_id,
    model_kwargs={
        "dtype": torch.float16 if torch.cuda.is_available() else torch.float32,
        "low_cpu_mem_usage": True,
    },
    device_map="auto", 
)

# 3. Define your prompt
messages = [
    {"role": "user", "content": "Explain the concept of recursion to a 5-year-old."},
]

# 4. Run the generation
print("\nGenerating response...")
outputs = pipe(
    messages,
    max_new_tokens=256,
    do_sample=True,
    temperature=0.7,
)

# 5. Print the result
generated_text = outputs[0]["generated_text"][-1]["content"]
print("-" * 50)
print(f"Prompt: {messages[0]['content']}")
print("-" * 50)
print(f"Response:\n{generated_text}")
print("-" * 50)
