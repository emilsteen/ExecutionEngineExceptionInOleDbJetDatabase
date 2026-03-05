using System.Data.OleDb;

namespace OleDbJetApp;

/// <summary>
/// Console application that reproduces System.ExecutionEngineException in System.Data.OleDb
/// when performing repeated INSERT/SELECT/UPDATE operations with multiple DateTime fields
/// against a Microsoft Jet (Access .mdb) database.
/// </summary>
internal class Program
{
    // Configurable: number of DateTime (stamp) fields in the table
    private const int NumberOfDateTimeFields = 20;

    // Configurable: number of insert/select/update loop iterations
    private const int NumberOfIterations = 1000;

    private const string EmptyDbFileName = "empty.mdb";
    private const string WorkingDbFileName = "working.mdb";
    private const string TableName = "TestTable";
    private const string IdFieldName = "Id";
    private const string StampFieldPrefix = "Stamp";

    static void Main(string[] args)
    {
        Console.WriteLine("OleDb Jet Database Test");
        Console.WriteLine($"DateTime fields: {NumberOfDateTimeFields}");
        Console.WriteLine($"Iterations: {NumberOfIterations}");
        Console.WriteLine();

        // Step 1: Copy the empty database template to the working database path
        string emptyDbPath = Path.Combine(AppContext.BaseDirectory, EmptyDbFileName);
        string workingDbPath = Path.Combine(AppContext.BaseDirectory, WorkingDbFileName);

        if (!File.Exists(emptyDbPath))
        {
            Console.Error.WriteLine($"Error: '{emptyDbPath}' not found.");
            Environment.Exit(1);
        }

        Console.WriteLine($"Copying '{EmptyDbFileName}' -> '{WorkingDbFileName}'...");
        File.Copy(emptyDbPath, workingDbPath, overwrite: true);

        string connectionString = $"Provider=Microsoft.Jet.OLEDB.4.0;Data Source={workingDbPath};";

        using var connection = new OleDbConnection(connectionString);
        connection.Open();
        Console.WriteLine("Connection opened.");

        // Step 2: Create the table with an Id field and the configured DateTime fields
        CreateTable(connection);

        // Step 3: Loop: Insert, Select, Update
        Console.WriteLine($"Starting {NumberOfIterations} iterations...");
        for (int i = 0; i < NumberOfIterations; i++)
        {
            InsertRecord(connection, i);
            SelectRecord(connection, i);
            UpdateRecord(connection, i);

            if ((i + 1) % 100 == 0)
            {
                Console.WriteLine($"  Completed {i + 1} iterations.");
            }
        }

        Console.WriteLine();
        Console.WriteLine("Done! No System.ExecutionEngineException occurred.");
    }

    /// <summary>
    /// Creates the test table with an integer Id field and <see cref="NumberOfDateTimeFields"/>
    /// DateTime stamp fields.
    /// </summary>
    private static void CreateTable(OleDbConnection connection)
    {
        var columns = new System.Text.StringBuilder();
        // TableName, IdFieldName and StampFieldPrefix are compile-time constants, not user input.
        columns.Append($"[{IdFieldName}] INTEGER NOT NULL PRIMARY KEY");
        for (int i = 1; i <= NumberOfDateTimeFields; i++)
        {
            columns.Append($", [{StampFieldPrefix}{i}] DATETIME");
        }

        string sql = $"CREATE TABLE [{TableName}] ({columns})";
        Console.WriteLine($"Creating table: {sql}");

        using var cmd = new OleDbCommand(sql, connection);
        cmd.ExecuteNonQuery();
        Console.WriteLine("Table created.");
    }

    /// <summary>
    /// Builds a comma-separated list of all stamp field names.
    /// </summary>
    private static string GetStampFieldList()
    {
        return string.Join(", ", Enumerable.Range(1, NumberOfDateTimeFields)
            .Select(i => $"[{StampFieldPrefix}{i}]"));
    }

    /// <summary>
    /// Builds a comma-separated list of parameter placeholders for the stamp fields.
    /// </summary>
    private static string GetStampParamPlaceholders()
    {
        return string.Join(", ", Enumerable.Repeat("?", NumberOfDateTimeFields));
    }

    /// <summary>
    /// Adds all stamp field parameters to the given command with the provided DateTime value.
    /// OleDb uses positional parameters (?); the parameter names passed to AddWithValue are
    /// metadata only and are ignored at runtime.
    /// </summary>
    private static void AddStampParameters(OleDbCommand cmd, DateTime value)
    {
        for (int i = 1; i <= NumberOfDateTimeFields; i++)
        {
            // Parameter name is ignored by OleDb (positional binding); value order matters.
            cmd.Parameters.AddWithValue($"@{StampFieldPrefix}{i}", value);
        }
    }

    /// <summary>
    /// Inserts a new record with the given id and the current UTC time for all stamp fields.
    /// </summary>
    private static void InsertRecord(OleDbConnection connection, int id)
    {
        string fields = $"[{IdFieldName}], {GetStampFieldList()}";
        string values = $"?, {GetStampParamPlaceholders()}";

        string sql = $"INSERT INTO [{TableName}] ({fields}) VALUES ({values})";
        using var cmd = new OleDbCommand(sql, connection);

        cmd.Parameters.AddWithValue("@Id", id);
        AddStampParameters(cmd, DateTime.UtcNow);

        cmd.ExecuteNonQuery();
    }

    /// <summary>
    /// Selects the record with the given id and reads all fields.
    /// </summary>
    private static void SelectRecord(OleDbConnection connection, int id)
    {
        string sql = $"SELECT [{IdFieldName}], {GetStampFieldList()} FROM [{TableName}] WHERE [{IdFieldName}] = ?";
        using var cmd = new OleDbCommand(sql, connection);
        cmd.Parameters.AddWithValue("@Id", id);

        using var reader = cmd.ExecuteReader();
        if (reader.Read())
        {
            // Read the id field
            _ = reader.GetInt32(0);

            // Read all stamp fields
            for (int i = 1; i <= NumberOfDateTimeFields; i++)
            {
                if (!reader.IsDBNull(i))
                {
                    _ = reader.GetDateTime(i);
                }
            }
        }
    }

    /// <summary>
    /// Updates all stamp fields of the record with the given id to the current UTC time.
    /// </summary>
    private static void UpdateRecord(OleDbConnection connection, int id)
    {
        string setClause = string.Join(", ", Enumerable.Range(1, NumberOfDateTimeFields)
            .Select(i => $"[{StampFieldPrefix}{i}] = ?"));

        string sql = $"UPDATE [{TableName}] SET {setClause} WHERE [{IdFieldName}] = ?";
        using var cmd = new OleDbCommand(sql, connection);

        AddStampParameters(cmd, DateTime.UtcNow);
        cmd.Parameters.AddWithValue("@Id", id);

        cmd.ExecuteNonQuery();
    }
}
