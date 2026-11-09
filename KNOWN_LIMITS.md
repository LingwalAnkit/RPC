# Known limits

Behaviour that is deliberate or not yet handled. Each item is either accepted
for now or planned for later. Items fixed in the baseline are not listed here.

1. **A call with no timeout waits forever** if the server never replies. This
   matches gRPC without a deadline. Callers should pass a timeout.

2. **The server waits for the peer's acknowledgement with no limit of its own.**
   After a reply is sent, `respond()` waits on `SendStream::stopped()`, which
   completes only when the peer acknowledges all data. A peer that never
   acknowledges can hold a server task until the 10-second shutdown drain ends it.
   Not tested. A per-reply time limit would fix it.

3. **Unknown error codes become `Internal`.** A newer peer that sends a code this
   build does not know is reported as `Internal`, with its message kept.

4. **Deadlines have millisecond resolution.** A timeout under 1 ms is sent as
   1 ms. A deadline of 0 means "no deadline", so a zero-length timeout cannot be sent.

5. **The `END_STREAM` flag is written but not checked on receipt.** Streams end
   when the stream closes, so this has no effect yet.

6. **Logging uses `eprintln!` with no levels.** Replace with a logging crate once
   there is more to log.

7. **No version negotiation.** The header has a version byte, and a mismatch is
   rejected. There is no way for two versions to agree on a common one.

8. **Two tests rely on timing.** The disappearing-server test waits 200 ms before
   killing the server, which assumes the call has started by then. The
   cancellation test polls for up to 2 seconds.
