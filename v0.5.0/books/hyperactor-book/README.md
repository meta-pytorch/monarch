# Hyperactor Documentation Book

This is the development documentation for the hyperactor system, built using [`mdBook`](https://rust-lang.github.io/mdBook/).

Contents

- [Hyperactor](src/introduction.html)

- [Actors](src/actors/index.html)
- [Macros](src/macros/index.html)
- [Channels](src/channels/index.html)
- [Mailboxes and Routers](src/mailboxes/index.html)
- [References](src/references/index.html)
- [Appendix](src/appendix/index.html)
- [Summary](src/SUMMARY.html)

## Running the Book

### On the **Server**

To run the book on a remote server (e.g., `devgpu004`):

```
x2ssh devgpu004.rva5.facebook.com
tmux new -s mdbook
cd ~/fbsource/fbcode/monarch/books/hyperactor-book
mdbook serve
```

Then detach with Ctrl+b, then d.

### On the **Client**

To access the remote book from your local browser:

```
autossh -M 0 -N -L 3000:localhost:3000 devgpu004.rva5.facebook.com
```

Then open http://localhost:3000 in your browser.

**Note**: If you don't have autossh installed, you can install it with:

```
brew install autossh
```

### Notes

- The source is located in src/, with structure defined in SUMMARY.md.
- The book will auto-reload in the browser on edits.

## Cleaning Up

To shut down the book server:

### Option 1: Reattach and stop

```
x2ssh devgpu004.rva5.facebook.com
tmux attach -t mdbook
```

Inside the session:

- Press Ctrl+C to stop mdbook serve
- Then type exit to close the shell and terminate the tmux session

### Option 2: Kill the session directly

If you don't want to reattach, you can kill the session from a new shell:

```
x2ssh devgpu004.rva5.facebook.com
tmux kill-session -t mdbook
```

### Optional: View active tmux sessions

```
tmux ls
```

Use this to check whether the mdbook session is still running.