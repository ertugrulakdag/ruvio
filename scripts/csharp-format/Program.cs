// Blank-line and line-break rules for the repository's C#, applied to the syntax tree so code inside
// strings, comments and preprocessor regions is never touched.
//
//   dotnet run --project scripts/csharp-format -- [--check] [paths...]
//
// Without paths it formats clients/csharp, integrations/dotnet and tests (generated *.g.cs, bin/ and
// obj/ are skipped). --check changes nothing and exits 1 when a file would change.
//
// Rules, among the statements of one block or switch section (and among a type's members):
// 1. A statement after a `}` that closes a block, and a member after a body that spans several
//    lines, gets one blank line before it. A `}` followed by `catch`, `finally`, `else` or another
//    `}` is part of the same statement and is not a separator.
// 2. An `if`, `for`, `foreach`, `while`, `do` or `switch` statement gets one blank line before it
//    (except a guard: an `if` with a single unbraced statement and no `else`, which may follow the
//    line it checks), and the statement after any of them gets one, braces or not.
// 3. A `return` that is not the first statement of its block gets one blank line before it.
// 4. The body of an `if`, `else`, loop, `using`, `lock` or `fixed` statement starts on its own line.
// 5. A rule asks for exactly one blank line: several rules never add two, and two or more blank lines
//    become one. Blank lines no rule asks for are kept (one at most); none are removed.
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

bool check = args.Contains("--check");
string[] roots = args.Where(a => a != "--check").ToArray();

if (roots.Length == 0)
{
    string repo = FindRepositoryRoot();

    roots = [Path.Combine(repo, "clients", "csharp"), Path.Combine(repo, "integrations", "dotnet"), Path.Combine(repo, "tests")];
}

int changed = 0;

foreach (string file in roots.SelectMany(Files).Order(StringComparer.Ordinal))
{
    string text = File.ReadAllText(file);
    string formatted = Layout.Apply(text);

    if (formatted == text)
        continue;

    changed++;
    Console.WriteLine(check ? $"would change: {file}" : $"formatted: {file}");

    if (!check)
        File.WriteAllText(file, formatted);
}

Console.WriteLine($"{changed} file(s) {(check ? "need formatting" : "changed")}");

return check && changed > 0 ? 1 : 0;

static IEnumerable<string> Files(string root)
{
    if (File.Exists(root))
        return [root];

    return Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)
        .Where(path => !path.EndsWith(".g.cs", StringComparison.Ordinal))
        .Where(path => !path.Split(Path.DirectorySeparatorChar).Any(part => part is "bin" or "obj" or "node_modules"));
}

static string FindRepositoryRoot()
{
    for (string? dir = Directory.GetCurrentDirectory(); dir is not null; dir = Path.GetDirectoryName(dir))
    {
        if (File.Exists(Path.Combine(dir, "Cargo.toml")) && Directory.Exists(Path.Combine(dir, "clients")))
            return dir;
    }

    throw new InvalidOperationException("run inside the repository or pass paths");
}

static class Layout
{
    public static string Apply(string text)
    {
        string newline = text.Contains("\r\n", StringComparison.Ordinal) ? "\r\n" : "\n";
        SyntaxTree tree = CSharpSyntaxTree.ParseText(text, new CSharpParseOptions(LanguageVersion.Preview));
        SyntaxNode root = tree.GetRoot();

        // Bodies on their own line first, so the blank-line pass sees the final lines.
        root = new BodyOnOwnLine(newline).Visit(root);
        root = new BlankLines(newline).Visit(root);

        return root.ToFullString();
    }
}

/// <summary>Rule 4: moves a body that shares the line of its `if (...)`, `else`, loop header, ... to the next line.</summary>
sealed class BodyOnOwnLine(string newline) : CSharpSyntaxRewriter
{
    public override SyntaxNode? VisitIfStatement(IfStatementSyntax node)
    {
        var visited = (IfStatementSyntax)base.VisitIfStatement(node)!;
        string indent = Indent(visited.IfKeyword);

        visited = Break(visited, visited.CloseParenToken, visited.Statement, indent,
            (n, close, body) => n.WithCloseParenToken(close).WithStatement(body));

        if (visited.Else is { } clause && clause.Statement is not IfStatementSyntax)
        {
            string elseIndent = Indent(clause.ElseKeyword);
            ElseClauseSyntax moved = Break(clause, clause.ElseKeyword, clause.Statement, elseIndent,
                (n, keyword, body) => n.WithElseKeyword(keyword).WithStatement(body));

            visited = visited.WithElse(moved);
        }

        return visited;
    }

    public override SyntaxNode? VisitForStatement(ForStatementSyntax node)
    {
        var n = (ForStatementSyntax)base.VisitForStatement(node)!;

        return Break(n, n.CloseParenToken, n.Statement, Indent(n.ForKeyword), (x, c, b) => x.WithCloseParenToken(c).WithStatement(b));
    }

    public override SyntaxNode? VisitForEachStatement(ForEachStatementSyntax node)
    {
        var n = (ForEachStatementSyntax)base.VisitForEachStatement(node)!;

        return Break(n, n.CloseParenToken, n.Statement, Indent(n.ForEachKeyword), (x, c, b) => x.WithCloseParenToken(c).WithStatement(b));
    }

    public override SyntaxNode? VisitForEachVariableStatement(ForEachVariableStatementSyntax node)
    {
        var n = (ForEachVariableStatementSyntax)base.VisitForEachVariableStatement(node)!;

        return Break(n, n.CloseParenToken, n.Statement, Indent(n.ForEachKeyword), (x, c, b) => x.WithCloseParenToken(c).WithStatement(b));
    }

    public override SyntaxNode? VisitWhileStatement(WhileStatementSyntax node)
    {
        var n = (WhileStatementSyntax)base.VisitWhileStatement(node)!;

        return Break(n, n.CloseParenToken, n.Statement, Indent(n.WhileKeyword), (x, c, b) => x.WithCloseParenToken(c).WithStatement(b));
    }

    public override SyntaxNode? VisitDoStatement(DoStatementSyntax node)
    {
        var n = (DoStatementSyntax)base.VisitDoStatement(node)!;

        return Break(n, n.DoKeyword, n.Statement, Indent(n.DoKeyword), (x, c, b) => x.WithDoKeyword(c).WithStatement(b));
    }

    public override SyntaxNode? VisitLockStatement(LockStatementSyntax node)
    {
        var n = (LockStatementSyntax)base.VisitLockStatement(node)!;

        return Break(n, n.CloseParenToken, n.Statement, Indent(n.LockKeyword), (x, c, b) => x.WithCloseParenToken(c).WithStatement(b));
    }

    public override SyntaxNode? VisitFixedStatement(FixedStatementSyntax node)
    {
        var n = (FixedStatementSyntax)base.VisitFixedStatement(node)!;

        return Break(n, n.CloseParenToken, n.Statement, Indent(n.FixedKeyword), (x, c, b) => x.WithCloseParenToken(c).WithStatement(b));
    }

    public override SyntaxNode? VisitUsingStatement(UsingStatementSyntax node)
    {
        var n = (UsingStatementSyntax)base.VisitUsingStatement(node)!;

        // `using (a) using (b) { }` stacks headers on purpose; only a non-using body is moved.
        if (n.Statement is UsingStatementSyntax)
            return n;

        return Break(n, n.CloseParenToken, n.Statement, Indent(n.UsingKeyword), (x, c, b) => x.WithCloseParenToken(c).WithStatement(b));
    }

    // Puts `body` on the line after `head` (indented one level past the statement) when it is on the
    // same line. A block body is left to the brace style; only single statements move.
    T Break<T>(T node, SyntaxToken head, StatementSyntax body, string indent, Func<T, SyntaxToken, StatementSyntax, T> rebuild)
        where T : SyntaxNode
    {
        if (body is BlockSyntax || HasNewline(head.TrailingTrivia) || HasNewline(body.GetLeadingTrivia()))
            return node;

        if (head.TrailingTrivia.Any(t => t.IsKind(SyntaxKind.SingleLineCommentTrivia) || t.IsKind(SyntaxKind.MultiLineCommentTrivia)))
            return node;

        SyntaxToken newHead = head.WithTrailingTrivia(SyntaxFactory.EndOfLine(newline));
        StatementSyntax newBody = body.WithLeadingTrivia(SyntaxFactory.Whitespace(indent + "    "));

        return rebuild(node, newHead, newBody);
    }

    static bool HasNewline(SyntaxTriviaList trivia) => trivia.Any(t => t.IsKind(SyntaxKind.EndOfLineTrivia));

    // The whitespace that starts the line `token` is on.
    static string Indent(SyntaxToken token)
    {
        SyntaxToken first = token;

        // Walk back to the first token on this line.
        while (true)
        {
            SyntaxToken previous = first.GetPreviousToken();

            if (previous.IsKind(SyntaxKind.None) || HasNewline(previous.TrailingTrivia) || HasNewline(first.LeadingTrivia))
                break;

            first = previous;
        }

        SyntaxTriviaList leading = first.LeadingTrivia;

        return leading.Count > 0 && leading[^1].IsKind(SyntaxKind.WhitespaceTrivia) ? leading[^1].ToString() : "";
    }
}

/// <summary>Rules 1, 2, 3 and 5: blank lines between sibling statements and members.</summary>
sealed class BlankLines(string newline) : CSharpSyntaxRewriter
{
    public override SyntaxNode? VisitBlock(BlockSyntax node)
    {
        var visited = (BlockSyntax)base.VisitBlock(node)!;

        return visited.WithStatements(Space(visited.Statements));
    }

    public override SyntaxNode? VisitSwitchSection(SwitchSectionSyntax node)
    {
        var visited = (SwitchSectionSyntax)base.VisitSwitchSection(node)!;

        return visited.WithStatements(Space(visited.Statements));
    }

    public override SyntaxNode? VisitClassDeclaration(ClassDeclarationSyntax node) =>
        SpaceMembers((ClassDeclarationSyntax)base.VisitClassDeclaration(node)!);

    public override SyntaxNode? VisitStructDeclaration(StructDeclarationSyntax node) =>
        SpaceMembers((StructDeclarationSyntax)base.VisitStructDeclaration(node)!);

    public override SyntaxNode? VisitRecordDeclaration(RecordDeclarationSyntax node) =>
        SpaceMembers((RecordDeclarationSyntax)base.VisitRecordDeclaration(node)!);

    public override SyntaxNode? VisitInterfaceDeclaration(InterfaceDeclarationSyntax node) =>
        SpaceMembers((InterfaceDeclarationSyntax)base.VisitInterfaceDeclaration(node)!);

    public override SyntaxNode? VisitCompilationUnit(CompilationUnitSyntax node)
    {
        var visited = (CompilationUnitSyntax)base.VisitCompilationUnit(node)!;
        var members = visited.Members;
        bool topLevel = members.Any(m => m is GlobalStatementSyntax);

        // Top-level statements follow the statement rules; type declarations the member rule.
        return visited.WithMembers(topLevel ? SpaceGlobals(members) : SpaceMemberList(members));
    }

    public override SyntaxNode? VisitNamespaceDeclaration(NamespaceDeclarationSyntax node)
    {
        var visited = (NamespaceDeclarationSyntax)base.VisitNamespaceDeclaration(node)!;

        return visited.WithMembers(SpaceMemberList(visited.Members));
    }

    public override SyntaxNode? VisitFileScopedNamespaceDeclaration(FileScopedNamespaceDeclarationSyntax node)
    {
        var visited = (FileScopedNamespaceDeclarationSyntax)base.VisitFileScopedNamespaceDeclaration(node)!;

        return visited.WithMembers(SpaceMemberList(visited.Members));
    }

    T SpaceMembers<T>(T type) where T : TypeDeclarationSyntax =>
        (T)type.WithMembers(SpaceMemberList(type.Members));

    SyntaxList<MemberDeclarationSyntax> SpaceMemberList(SyntaxList<MemberDeclarationSyntax> members)
    {
        var result = new List<MemberDeclarationSyntax>(members.Count);

        for (int i = 0; i < members.Count; i++)
        {
            bool want = i > 0 && EndsWithBlock(members[i - 1]);

            result.Add(Normalize(members[i], want));
        }

        return SyntaxFactory.List(result);
    }

    SyntaxList<MemberDeclarationSyntax> SpaceGlobals(SyntaxList<MemberDeclarationSyntax> members)
    {
        var result = new List<MemberDeclarationSyntax>(members.Count);

        for (int i = 0; i < members.Count; i++)
        {
            bool want = i > 0 && members[i] is GlobalStatementSyntax current && members[i - 1] is GlobalStatementSyntax previous
                ? WantsBlank(previous.Statement, current.Statement)
                : i > 0 && EndsWithBlock(members[i - 1]);

            result.Add(Normalize(members[i], want));
        }

        return SyntaxFactory.List(result);
    }

    SyntaxList<StatementSyntax> Space(SyntaxList<StatementSyntax> statements)
    {
        var result = new List<StatementSyntax>(statements.Count);

        for (int i = 0; i < statements.Count; i++)
        {
            // A statement that shares a line with the one before it (`{ a(); return b; }`) is left alone.
            if (i > 0 && !StartsLine(statements[i]))
            {
                result.Add(statements[i]);
                continue;
            }

            bool want = i > 0 && WantsBlank(statements[i - 1], statements[i]);

            result.Add(Normalize(statements[i], want));
        }

        return SyntaxFactory.List(result);
    }

    static bool WantsBlank(StatementSyntax previous, StatementSyntax current)
    {
        return EndsWithBrace(previous)
            || IsControl(previous)
            || (IsControl(current) && !IsGuard(current))
            || current is ReturnStatementSyntax;
    }

    // `if (x is null) return;` with no braces and no else: a guard that may follow the line it checks.
    static bool IsGuard(StatementSyntax statement) =>
        statement is IfStatementSyntax { Statement: not BlockSyntax, Else: null };

    static bool IsControl(StatementSyntax statement) =>
        statement is IfStatementSyntax or ForStatementSyntax or ForEachStatementSyntax or ForEachVariableStatementSyntax
            or WhileStatementSyntax or DoStatementSyntax or SwitchStatementSyntax;

    static bool StartsLine(SyntaxNode node) =>
        node.GetLeadingTrivia().Any(t => t.IsKind(SyntaxKind.EndOfLineTrivia))
        || node.GetFirstToken().GetPreviousToken().TrailingTrivia.Any(t => t.IsKind(SyntaxKind.EndOfLineTrivia));

    static bool EndsWithBrace(SyntaxNode node) => node.GetLastToken().IsKind(SyntaxKind.CloseBraceToken);

    // A member whose `}` closes a body over several lines; `int X { get; set; }` on one line is not one.
    static bool EndsWithBlock(SyntaxNode node) =>
        EndsWithBrace(node) && node.ToString().Contains('\n', StringComparison.Ordinal);

    // Leaves exactly one blank line before `node` when `want`, otherwise at most one; never touches
    // trivia that holds preprocessor directives.
    T Normalize<T>(T node, bool want) where T : SyntaxNode
    {
        SyntaxTriviaList leading = node.GetLeadingTrivia();

        if (leading.Any(t => t.IsDirective || t.IsKind(SyntaxKind.DisabledTextTrivia)))
            return node;

        // The run of whitespace and line ends before the first comment or the token itself.
        int run = 0;
        int blank = 0;

        while (run < leading.Count && (leading[run].IsKind(SyntaxKind.WhitespaceTrivia) || leading[run].IsKind(SyntaxKind.EndOfLineTrivia)))
        {
            if (leading[run].IsKind(SyntaxKind.EndOfLineTrivia))
                blank++;

            run++;
        }

        int keep = want ? 1 : Math.Min(blank, 1);

        if (keep == blank)
            return node;

        // Keep the indentation (the whitespace after the run's last line end).
        int lastEnd = -1;

        for (int i = 0; i < run; i++)
        {
            if (leading[i].IsKind(SyntaxKind.EndOfLineTrivia))
                lastEnd = i;
        }

        var rest = leading.Skip(lastEnd + 1).ToList();
        var rebuilt = new List<SyntaxTrivia>();

        for (int i = 0; i < keep; i++)
            rebuilt.Add(SyntaxFactory.EndOfLine(newline));

        rebuilt.AddRange(rest);

        return node.WithLeadingTrivia(rebuilt);
    }
}
