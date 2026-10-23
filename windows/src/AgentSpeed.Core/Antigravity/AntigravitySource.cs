using AgentSpeed.Core.Engine;
using AgentSpeed.Core.IO.Sqlite;
using AgentSpeed.Core.Parsing;

namespace AgentSpeed.Core.Antigravity;

/// <summary>WAL-aware polling of one client's conversation databases. Retains metrics only.</summary>
internal sealed class AntigravitySource(SourceKind kind, string root)
{
    private readonly Dictionary<string, Stamp> _stamps = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, ParsedResponse> _responses = new(StringComparer.Ordinal);
    private readonly Dictionary<string, AgentInfo> _agents = new(StringComparer.OrdinalIgnoreCase);
    private readonly Dictionary<string, int> _malformed = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _unreadable = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _unsupported = new(StringComparer.OrdinalIgnoreCase);

    public SourceKind Kind { get; } = kind;
    public string Root { get; } = root;
    public bool Available => Directory.Exists(Root);
    public int Unreadable => _unreadable.Count;
    public bool Unsupported => _unsupported.Count > 0;
    public int Malformed => _malformed.Values.Sum();
    public int SessionCount(long since) => _agents.Values.Count(a => a.LastLineMs >= since);

    public bool Refresh(long todayStart, Action<AgentInfo> onAgent, Action<ParsedResponse, bool> onResponse)
    {
        var changed = false;
        foreach (var path in Paths(Root))
        {
            try
            {
                var stamp = FileStamp(path);
                if (_stamps.TryGetValue(path, out var old) && old == stamp) continue;
                // Historic closed databases belong to HistoryAnalyzer, not the startup speed backlog.
                if (!_stamps.ContainsKey(path) && stamp.ModifiedMs < todayStart - 15 * 60_000) continue;
                var read = AntigravityDatabase.Read(path, Kind);
                if (_agents.GetValueOrDefault(path) != read.Agent)
                {
                    _agents[path] = read.Agent;
                    onAgent(read.Agent);
                    changed = true;
                }
                foreach (var response in read.Responses)
                {
                    if (response.EndMs < todayStart - 15 * 60_000) continue;
                    var previous = _responses.GetValueOrDefault(response.Key);
                    if (previous == response) continue;
                    _responses[response.Key] = response;
                    onResponse(response, previous is null);
                    changed = true;
                }
                _malformed[path] = read.Malformed;
                _unreadable.Remove(path);
                _unsupported.Remove(path);
                // Use the stamp BEFORE reading: a concurrent commit is picked up on the next poll.
                _stamps[path] = stamp;
            }
            catch (SqliteException e)
            {
                if (e.PrimaryCode == SqliteNative.Error) _unsupported.Add(path);
                else _unreadable.Add(path);
                changed = true;
            }
            catch (Exception e) when (e is IOException or UnauthorizedAccessException)
            {
                _unreadable.Add(path);
                changed = true;
            }
        }
        return changed;
    }

    public static IEnumerable<string> Paths(string root)
    {
        try { return Directory.Exists(root) ? Directory.GetFiles(root, "*.db") : []; }
        catch (Exception e) when (e is IOException or UnauthorizedAccessException) { return []; }
    }

    internal static Stamp FileStamp(string path)
    {
        var db = new FileInfo(path);
        var wal = new FileInfo(path + "-wal");
        return new Stamp(db.Length, db.LastWriteTimeUtc.Ticks, wal.Exists ? wal.Length : 0, wal.Exists ? wal.LastWriteTimeUtc.Ticks : 0);
    }

    internal readonly record struct Stamp(long Size, long Modified, long WalSize, long WalModified)
    {
        public long ModifiedMs => new DateTimeOffset(Math.Max(Modified, WalModified), TimeSpan.Zero).ToUnixTimeMilliseconds();
    }
}
