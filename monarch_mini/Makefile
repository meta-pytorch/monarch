LIB     = ../target/debug/libmonarch_mini.a
INCLUDE = .
OUT     = ../target/debug/hello

.PHONY: all run clean cargo

all: $(OUT)

# Always invoke cargo — it tracks Rust source changes itself.
cargo:
	cargo build -p monarch_mini

$(OUT): examples/hello.c cargo
	cc examples/hello.c $(LIB) -I$(INCLUDE) -o $(OUT)

run: $(OUT)
	$(OUT)

clean:
	rm -f $(OUT)
