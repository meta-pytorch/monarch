# Hyperactor Documentation Book

This is the development documentation for the hyperactor system, built using [`mdBook`](https://rust-lang.github.io/mdBook/).

Contents

- [Hyperactor](src/introduction.html)

- [Actors](src/actors/index.html)
- [Macros](src/macros/index.html)
- [Channels](src/channels/index.html)
- [Mailboxes and Routers](src/mailboxes/index.html)
- [Procs](src/procs/index.html)
- [References](src/references/index.html)
- [Remote Supervision and Rendezvous](src/remote_supervision.html)
- [Appendix](src/appendix/index.html)
- [References](src/references/index.html)
- [Mailboxes and Routers](src/mailboxes/index.html)
- [Channels](src/channels/index.html)
- [Procs](src/procs/index.html)

- [Overview](src/procs/index.html#overview)
- [Key Components](src/procs/index.html#key-components)
- [See Also](src/procs/index.html#see-also)
- [Actors](src/actors/index.html)
- [Remote Supervision and Rendezvous](src/remote_supervision.html)

- [Intended usage](src/remote_supervision.html#intended-usage)
- [Remote supervision](src/remote_supervision.html#remote-supervision)
- [Link protocol](src/remote_supervision.html#link-protocol)
- [Keepalive link](src/remote_supervision.html#keepalive-link)
- [Child and link failures](src/remote_supervision.html#child-and-link-failures)
- [Stop and unlink](src/remote_supervision.html#stop-and-unlink)
- [Rendezvous tokens](src/remote_supervision.html#rendezvous-tokens)
- [Public surface](src/remote_supervision.html#public-surface)
- [Macros](src/macros/index.html)

- [Macro Summary](src/macros/index.html#macro-summary)
- [Appendix](src/appendix/index.html)

## Running the Book

### On the **Server**

To run the book on a remote server (e.g., `devgpu004`):

```
x2ssh devgpu004.rva5.facebook.com
tmux new -s mdbook
cd ~/fbsource/fbcode/monarch/docs/source/books/hyperactor-book
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