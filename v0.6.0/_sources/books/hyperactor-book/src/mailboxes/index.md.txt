# Mailboxes and Routers

Mailboxes are the foundation of message delivery in hyperactor. They coordinate typed ports, routing logic, forwarding, and delivery infrastructure for distributed actors.

This chapter introduces the components of the mailbox subsystem:

- [Ports](ports.md): typed channels for local message delivery
- [MailboxSender](mailbox_sender.md): trait-based abstraction for message posting
- [Reconfigurable Senders](reconfigurable_sender.md): deferred wiring and dynamic configuration
- [MailboxServer](mailbox_server.md): bridging incoming message streams into mailboxes
- [MailboxClient](mailbox_client.md): buffering, forwarding, and failure reporting
- [Mailbox](mailbox.md): port registration, binding, and routing
- [Delivery Semantics](delivery.md): envelopes, delivery errors, and failure handling
- [Multiplexers](multiplexer.md): port-level dispatch to local mailboxes
- [Routers](routers.md): prefix-based routing to local or remote destinations
