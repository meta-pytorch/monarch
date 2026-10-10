# Mailboxes and Routers

Mailboxes are the foundation of message delivery in hyperactor. They coordinate typed ports, routing logic, forwarding, and delivery infrastructure for distributed actors.

This chapter introduces the components of the mailbox subsystem:

- [Ports](ports.html): typed channels for local message delivery
- [MailboxSender](mailbox_sender.html): trait-based abstraction for message posting
- [Reconfigurable Senders](reconfigurable_sender.html): deferred wiring and dynamic configuration
- [MailboxServer](mailbox_server.html): bridging incoming message streams into mailboxes
- [MailboxClient](mailbox_client.html): buffering, forwarding, and failure reporting
- [Mailbox](mailbox.html): port registration, binding, and routing
- [Delivery Semantics](delivery.html): envelopes, delivery errors, and failure handling
- [Multiplexers](multiplexer.html): port-level dispatch to local mailboxes
- [Routers](routers.html): prefix-based routing to local or remote destinations