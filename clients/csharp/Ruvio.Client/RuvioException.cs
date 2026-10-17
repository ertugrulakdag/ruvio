namespace Ruvio.Client;

/// <summary>
/// A Ruvio server error reply, or a RESP framing failure on the connection.
/// </summary>
/// <remarks>
/// <para>
/// When <see cref="IsServerReply"/> is true the server answered with an error such as <c>WRONGTYPE</c>,
/// <c>CROSSSLOT</c> or <c>ERR ...</c>; the message holds the server's text and the connection is still usable.
/// A framing failure or a socket that closed mid-command is not a server reply, and the connection must be reopened.
/// </para>
/// <para>
/// <see cref="ClosedBeforeReply"/> tells a read that is safe to resend from a write that may have run.
/// </para>
/// </remarks>
/// <param name="message">The server's error text or a description of the failure.</param>
/// <param name="isServerReply">True when the server sent a RESP error reply.</param>
/// <param name="inner">The exception that caused this one, such as a socket error.</param>
/// <param name="closedBeforeReply">True when the socket closed before any byte of this command's reply arrived.</param>
/// <example>
/// <code>
/// try
/// {
///     await db.IncrementAsync("not-a-number-key");
/// }
/// catch (RuvioException ex) when (ex.IsServerReply)
/// {
///     Console.WriteLine(ex.Message);   // the server's error text
/// }
/// </code>
/// </example>
public sealed class RuvioException(string message, bool isServerReply = false, Exception? inner = null, bool closedBeforeReply = false) : Exception(message, inner)
{

    /// <summary>
    /// True when Ruvio returned a RESP error and the next command can still run.
    /// </summary>
    public bool IsServerReply { get; } = isServerReply;

    /// <summary>
    /// True when the socket closed before any byte of this command's reply arrived.
    /// The client does not send it again; the caller may resend a read. A write may already have run.
    /// </summary>
    public bool ClosedBeforeReply { get; } = closedBeforeReply;
}
