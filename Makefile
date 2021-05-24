all: develop

develop:
	cargo build

release:
	@make clean
	cargo build --release

install-dev:
	cp target/debug/rs3 /usr/local/bin/

install:
	cp target/release/rs3 /usr/local/bin/

clean:
	cargo clean
