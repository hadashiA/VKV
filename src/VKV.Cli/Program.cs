using System.Buffers.Binary;
using System.Text;
using ConsoleAppFramework;
using MessagePack;
using Spectre.Console;
using VKV;

var app = ConsoleApp.Create();
app.Add<Commands>();
app.Run(args);

public class Commands
{
    /// <summary>
    /// Open a VKV database file and start an interactive session or execute a command.
    /// </summary>
    /// <param name="file">Path to the .vkv file</param>
    /// <param name="table">-t, Table name to use (optional, uses first table if not specified)</param>
    /// <param name="command">Command to execute (e.g., "get mykey"). If not specified, starts interactive session.</param>
    [Command("")]
    public async Task Interactive(string file, string? table = null, [Argument] params string[] command)
    {
        if (!File.Exists(file))
        {
            AnsiConsole.MarkupLine($"[red]Error:[/] File not found: {file}");
            return;
        }

        var fileName = Path.GetFileName(file);

        using var db = await ReadOnlyDatabase.OpenFileAsync(file);

        var tableNames = db.Catalog.TableDescriptors.Keys.ToList();
        if (tableNames.Count == 0)
        {
            AnsiConsole.MarkupLine("[red]Error:[/] No tables found in database.");
            return;
        }

        var currentTableName = table ?? tableNames[0];
        if (!db.Catalog.TableDescriptors.ContainsKey(currentTableName))
        {
            AnsiConsole.MarkupLine($"[red]Error:[/] Table '{currentTableName}' not found.");
            AnsiConsole.MarkupLine($"Available tables: {string.Join(", ", tableNames)}");
            return;
        }

        var currentTable = db.GetTable(currentTableName);

        // One-shot command mode
        if (command.Length > 0)
        {
            await ExecuteCommand(db, currentTable, command[0], command.Skip(1).ToArray());
            return;
        }

        // Interactive mode
        AnsiConsole.MarkupLine($"[green]Connected to[/] [yellow]{fileName}[/]");
        AnsiConsole.MarkupLine($"[dim]Table: {currentTableName} | Type 'help' for commands[/]");
        AnsiConsole.WriteLine();

        while (true)
        {
            var prompt = $"[blue]{fileName}[/][dim]({currentTableName})>[/] ";
            AnsiConsole.Markup(prompt);

            var input = Console.ReadLine();
            if (string.IsNullOrWhiteSpace(input))
                continue;

            var parts = ParseCommand(input);
            if (parts.Length == 0)
                continue;

            var cmd = parts[0].ToLowerInvariant();
            var cmdArgs = parts.Skip(1).ToArray();

            switch (cmd)
            {
                case "quit":
                case "exit":
                case "q":
                    AnsiConsole.MarkupLine("[dim]Bye![/]");
                    return;

                case "use":
                    if (cmdArgs.Length == 0)
                    {
                        AnsiConsole.MarkupLine("[red]Usage:[/] use <table_name>");
                    }
                    else
                    {
                        var newTableName = cmdArgs[0];
                        if (db.Catalog.TableDescriptors.ContainsKey(newTableName))
                        {
                            currentTableName = newTableName;
                            currentTable = db.GetTable(currentTableName);
                            AnsiConsole.MarkupLine($"[green]Switched to table:[/] {currentTableName}");
                        }
                        else
                        {
                            AnsiConsole.MarkupLine($"[red]Error:[/] Table '{newTableName}' not found.");
                        }
                    }
                    break;

                default:
                    await ExecuteCommand(db, currentTable, cmd, cmdArgs);
                    break;
            }
        }
    }

    static async Task ExecuteCommand(ReadOnlyDatabase db, ReadOnlyTable table, string command, string[] args)
    {
        switch (command.ToLowerInvariant())
        {
            case "help":
            case "?":
                PrintHelp();
                break;

            case "get":
                await ExecuteGet(table, args);
                break;

            case "tables":
                ExecuteTables(db);
                break;

            case "info":
                ExecuteInfo(db, table);
                break;

            case "scan":
                await ExecuteScan(table, args);
                break;

            case "count":
                ExecuteCount(table);
                break;

            default:
                AnsiConsole.MarkupLine($"[red]Unknown command:[/] {command}");
                AnsiConsole.MarkupLine("[dim]Type 'help' for available commands.[/]");
                break;
        }
    }

    static void PrintHelp()
    {
        var table = new Table();
        table.AddColumn("Command");
        table.AddColumn("Description");
        table.Border = TableBorder.Rounded;

        table.AddRow("[green]get[/] <key>", "Get value by key");
        table.AddRow("[green]scan[/] <count>", "Scan entries (default: 10)");
        table.AddRow("[green]count[/]", "Count all entries");
        table.AddRow("[green]tables[/]", "List all tables");
        table.AddRow("[green]use[/] <table>", "Switch to another table");
        table.AddRow("[green]info[/]", "Show database info");
        table.AddRow("[green]help[/]", "Show this help");
        table.AddRow("[green]quit[/]", "Exit the session");

        AnsiConsole.Write(table);
    }

    static async Task ExecuteGet(ReadOnlyTable table, string[] args)
    {
        if (args.Length == 0)
        {
            AnsiConsole.MarkupLine("[red]Usage:[/] get <key>");
            return;
        }

        var keyStr = args[0];
        if (!TryParseKey(keyStr, table.KeyEncoding, out var keyBytes))
        {
            AnsiConsole.MarkupLine($"[red]Error:[/] Invalid key format for encoding '{table.KeyEncoding.Id}'");
            return;
        }

        using var result = await table.GetAsync(new ReadOnlyMemory<byte>(keyBytes));

        if (!result.HasValue)
        {
            AnsiConsole.MarkupLine("[yellow](nil)[/]");
            return;
        }

        var valueBytes = result.Value.Memory.Span;
        var valueStr = TryDecodeValue(valueBytes);

        if (valueStr != null)
        {
            AnsiConsole.MarkupLine($"[green]{EscapeMarkup(valueStr)}[/]");
        }
        else
        {
            // Show as hex dump
            AnsiConsole.MarkupLine($"[dim](binary, {valueBytes.Length} bytes)[/]");
            PrintHexDump(valueBytes);
        }
    }

    static void ExecuteTables(ReadOnlyDatabase db)
    {
        var table = new Table();
        table.AddColumn("#");
        table.AddColumn("Table Name");
        table.AddColumn("Key Encoding");
        table.AddColumn("Indexes");
        table.Border = TableBorder.Rounded;

        var i = 1;
        foreach (var (name, descriptor) in db.Catalog.TableDescriptors)
        {
            var indexCount = descriptor.IndexDescriptors.Count;
            table.AddRow(
                i.ToString(),
                $"[green]{name}[/]",
                descriptor.PrimaryKeyDescriptor.KeyEncoding.Id,
                indexCount > 0 ? indexCount.ToString() : "-");
            i++;
        }

        AnsiConsole.Write(table);
    }

    static void ExecuteInfo(ReadOnlyDatabase db, ReadOnlyTable currentTable)
    {
        var table = new Table();
        table.AddColumn("Property");
        table.AddColumn("Value");
        table.Border = TableBorder.Rounded;

        table.AddRow("Page Size", $"{db.Catalog.PageSize} bytes");
        table.AddRow("Table Count", db.Catalog.TableDescriptors.Count.ToString());
        table.AddRow("Current Table", currentTable.Name);
        table.AddRow("Key Encoding", currentTable.KeyEncoding.Id);

        if (db.Catalog.Filters is { Count: > 0 })
        {
            var filterNames = string.Join(", ", db.Catalog.Filters.Select(f => f.GetType().Name));
            table.AddRow("Filters", filterNames);
        }

        AnsiConsole.Write(table);
    }

    static async Task ExecuteScan(ReadOnlyTable table, string[] args)
    {
        var count = 10;
        if (args.Length > 0 && int.TryParse(args[0], out var parsed))
        {
            count = parsed;
        }

        var iterator = table.CreateIterator();
        var displayed = 0;

        while (await iterator.MoveNextAsync() && displayed < count)
        {
            var value = iterator.Current;

            var valueStr = TryDecodeValue(value.Span);
            var valueDisplay = valueStr != null
                ? EscapeMarkup(Truncate(valueStr, 80))
                : $"(binary, {value.Length} bytes)";

            AnsiConsole.MarkupLine($"[dim]{displayed + 1})[/] [green]{valueDisplay}[/]");
            displayed++;
        }

        if (displayed == 0)
        {
            AnsiConsole.MarkupLine("[yellow](empty)[/]");
        }
        else
        {
            AnsiConsole.MarkupLine($"[dim]Displayed {displayed} entries[/]");
        }
    }

    static void ExecuteCount(ReadOnlyTable table)
    {
        var iterator = table.CreateIterator();
        var count = 0;

        while (iterator.MoveNext())
        {
            count++;
        }

        AnsiConsole.MarkupLine($"[green](integer)[/] {count}");
    }

    static string[] ParseCommand(string input)
    {
        var parts = new List<string>();
        var current = new StringBuilder();
        var inQuotes = false;
        var escapeNext = false;

        foreach (var c in input)
        {
            if (escapeNext)
            {
                current.Append(c);
                escapeNext = false;
                continue;
            }

            if (c == '\\')
            {
                escapeNext = true;
                continue;
            }

            if (c == '"')
            {
                inQuotes = !inQuotes;
                continue;
            }

            if (char.IsWhiteSpace(c) && !inQuotes)
            {
                if (current.Length > 0)
                {
                    parts.Add(current.ToString());
                    current.Clear();
                }
                continue;
            }

            current.Append(c);
        }

        if (current.Length > 0)
        {
            parts.Add(current.ToString());
        }

        return parts.ToArray();
    }

    static string? TryDecodeValue(ReadOnlySpan<byte> bytes)
    {
        // Try MessagePack first
        var jsonStr = TryDecodeAsMessagePackJson(bytes);
        if (jsonStr != null)
            return jsonStr;

        // Fall back to UTF-8 string
        return TryDecodeAsUtf8(bytes);
    }

    static string? TryDecodeAsMessagePackJson(ReadOnlySpan<byte> bytes)
    {
        try
        {
            return MessagePackSerializer.ConvertToJson(bytes.ToArray());
        }
        catch
        {
            return null;
        }
    }

    static string? TryDecodeAsUtf8(ReadOnlySpan<byte> bytes)
    {
        try
        {
            var str = Encoding.UTF8.GetString(bytes);
            // Check if all characters are printable
            foreach (var c in str)
            {
                if (char.IsControl(c) && c != '\n' && c != '\r' && c != '\t')
                    return null;
            }
            return str;
        }
        catch
        {
            return null;
        }
    }

    static void PrintHexDump(ReadOnlySpan<byte> bytes)
    {
        const int bytesPerLine = 16;
        for (var i = 0; i < bytes.Length; i += bytesPerLine)
        {
            var line = bytes.Slice(i, Math.Min(bytesPerLine, bytes.Length - i));
            var hex = Convert.ToHexString(line);
            var hexFormatted = string.Join(" ", Enumerable.Range(0, hex.Length / 2).Select(j => hex.Substring(j * 2, 2)));
            AnsiConsole.MarkupLine($"[dim]{i:X8}[/]  {hexFormatted}");

            if (i >= 64)
            {
                AnsiConsole.MarkupLine($"[dim]... ({bytes.Length - i} more bytes)[/]");
                break;
            }
        }
    }

    static string EscapeMarkup(string text)
    {
        return text.Replace("[", "[[").Replace("]", "]]");
    }

    static string Truncate(string text, int maxLength)
    {
        if (text.Length <= maxLength)
            return text;
        return text[..(maxLength - 3)] + "...";
    }

    /// <summary>
    /// Parse key string based on KeyEncoding using IKeyEncoding.TryEncode.
    /// - i64: parse as integer, then encode
    /// - ascii/u8: encode string directly
    /// </summary>
    static bool TryParseKey(string keyStr, IKeyEncoding encoding, out byte[] keyBytes)
    {
        switch (encoding.Id)
        {
            case "i64":
                if (long.TryParse(keyStr, out var longKey))
                {
                    var maxLen = encoding.GetMaxEncodedByteCount(longKey);
                    keyBytes = new byte[maxLen];
                    return encoding.TryEncode(longKey, keyBytes, out _);
                }
                keyBytes = [];
                return false;

            case "ascii":
            case "u8":
            default:
                var maxBytes = encoding.GetMaxEncodedByteCount(keyStr);
                keyBytes = new byte[maxBytes];
                if (encoding.TryEncode(keyStr, keyBytes, out var bytesWritten))
                {
                    if (bytesWritten < keyBytes.Length)
                        keyBytes = keyBytes[..bytesWritten];
                    return true;
                }
                keyBytes = [];
                return false;
        }
    }

    /// <summary>
    /// Format key bytes based on KeyEncoding for display.
    /// - i64: format as integer
    /// - ascii/u8: format as string
    /// </summary>
    static string FormatKey(ReadOnlySpan<byte> keyBytes, IKeyEncoding encoding)
    {
        switch (encoding.Id)
        {
            case "i64":
                if (keyBytes.Length >= sizeof(long))
                {
                    var value = BinaryPrimitives.ReadInt64LittleEndian(keyBytes);
                    return value.ToString();
                }
                return $"(hex:{Convert.ToHexString(keyBytes)})";

            case "ascii":
                return Encoding.ASCII.GetString(keyBytes);

            case "u8":
            default:
                return TryDecodeAsUtf8(keyBytes) ?? $"(hex:{Convert.ToHexString(keyBytes)})";
        }
    }
}
