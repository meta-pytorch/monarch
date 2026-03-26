# Hyperactor Documentation Book

This is the development documentation for the hyperactor system, built using [`mdBook`](https://rust-lang.github.io/mdBook/).

```{toctree}
:maxdepth: 2
:caption: Contents

./src/introduction
./src/refrences
macros
mailbox
channels
actors
summary
```

## Running the Book

### On the **Server**

To run the book on a remote server (e.g., `devgpu004`):

```bash
x2ssh devgpu004.rva5.facebook.com
tmux new -s mdbook
cd ~/fbsource/fbcode/monarch/books/hyperactor-book
mdbook serve
```
Then detach with Ctrl+b, then d.

### On the **Client**

To access the remote book from your local browser:
```bash
autossh -M 0 -N -L 3000:localhost:3000 devgpu004.rva5.facebook.com
```
Then open http://localhost:3000 in your browser.

**Note**: If you don’t have autossh installed, you can install it with:
```bash
brew install autossh
```

### Notes

- The source is located in src/, with structure defined in SUMMARY.md.
- The book will auto-reload in the browser on edits.

## Cleaning Up

To shut down the book server:

### Option 1: Reattach and stop

```bash
x2ssh devgpu004.rva5.facebook.com
tmux attach -t mdbook
```
Inside the session:
- Press Ctrl+C to stop mdbook serve
- Then type exit to close the shell and terminate the tmux session

### Option 2: Kill the session directly

If you don’t want to reattach, you can kill the session from a new shell:
```bash
x2ssh devgpu004.rva5.facebook.com
tmux kill-session -t mdbook
```

### Optional: View active tmux sessions
```bash
tmux ls
```
Use this to check whether the mdbook session is still running.
