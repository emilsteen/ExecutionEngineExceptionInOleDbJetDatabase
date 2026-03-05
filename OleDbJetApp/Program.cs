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
	private const int NumberOfStampFields = 20;

	// Configurable: number of insert/select/update loop iterations
	private const int NumberOfIterations = 1000;

	// When true, wraps all field and table names in SQL with square brackets ([name]).
	private const bool UseBrackets = true;

	private const string EmptyDbFileName = "empty.mdb";
	private const string WorkingDbFileName = "working.mdb";
	private const string TableName = "TestTable";
	private const string IdFieldName = "id_field";

	// All non-identity fields: drives CREATE TABLE, INSERT, SELECT, and UPDATE logic.
	private static readonly List<(string FieldName, string DataType)> FieldList =
	[
		.. Enumerable.Range(1, 5).Select(x => ($"int_field_{x}", "int")),
		.. Enumerable.Range(1, 5).Select(x => ($"guid_field_{x}", "guid")),
		.. Enumerable.Range(1, 5).Select(x => ($"str_field_{x}", "varchar")),
		.. Enumerable.Range(1, NumberOfStampFields).Select(x => ($"stamp_field_{x}", "datetime")),
	];

	static void Main(string[] args)
	{
		Console.WriteLine("OleDb Jet Database Test");
		Console.WriteLine($"Stamp fields: {NumberOfStampFields}");
		Console.WriteLine($"Iterations: {NumberOfIterations}");
		Console.WriteLine();


		// Step 1: Copy the empty database template to the working database path
		string emptyDbPath = Path.Combine(AppContext.BaseDirectory, EmptyDbFileName);
		string workingDbPath = Path.Combine(AppContext.BaseDirectory, WorkingDbFileName);

		string connectionString = $"Provider=Microsoft.ACE.OLEDB.12.0;Data Source={workingDbPath};";

		if (!File.Exists(emptyDbPath))
		{
			Console.Error.WriteLine($"Error: '{emptyDbPath}' not found.");
			Environment.Exit(1);
		}

		Console.WriteLine($"Copying '{EmptyDbFileName}' -> '{WorkingDbFileName}'...");
		File.Copy(emptyDbPath, workingDbPath, overwrite: true);

		using (var connection = new OleDbConnection(connectionString))
		{
			connection.Open();
			Console.WriteLine("Connection opened.");

			// Step 2: Create the table with an Id field and all fields from FieldList
			CreateTable(connection);
		}

		// Step 3: Loop: Insert, Select, Update
		Console.WriteLine($"Starting {NumberOfIterations} iterations...");
		for (int i = 0; i < NumberOfIterations; i++)
		{
			bool recordExists;
			string id = $"record-id-{i}";

			using (var connection = new OleDbConnection(connectionString))
			{
				connection.Open();
				recordExists = RecordExists(connection, id);
			}

			if (!recordExists)
			{
				using (var connection = new OleDbConnection(connectionString))
				{
					connection.Open();
					InsertRecord(connection, id);
				}
			}

			using (var connection = new OleDbConnection(connectionString))
			{
				connection.Open();
				SelectRecord(connection, id);
			}

			using (var connection = new OleDbConnection(connectionString))
			{
				connection.Open();
				UpdateRecord(connection, id);
			}

			if ((i + 1) % 10 == 0
				|| (i + 1) < 10)
			{
				Console.WriteLine($"  Completed {i + 1} iterations.");
			}
		}

		Console.WriteLine();
		Console.WriteLine("Done! No System.ExecutionEngineException occurred.");
	}

	/// <summary>
	/// Returns <paramref name="name"/> optionally wrapped in square brackets,
	/// controlled by <see cref="UseBrackets"/>.
	/// </summary>
	private static string Q(string name) => UseBrackets ? $"[{name}]" : name;

	/// <summary>
	/// Returns the SQL DDL type string for a given <paramref name="dataType"/> identifier.
	/// </summary>
	private static string GetSqlType(string dataType) => dataType.ToLowerInvariant() switch
	{
		"int"      => "INTEGER",
		"guid"     => "GUID",
		"varchar"  => "VARCHAR(255)",
		"datetime" => "DATETIME",
		_ => throw new NotSupportedException($"Unsupported data type: {dataType}"),
	};

	/// <summary>
	/// Returns the <see cref="OleDbType"/> for a given <paramref name="dataType"/> identifier.
	/// </summary>
	private static OleDbType GetOleDbType(string dataType) => dataType.ToLowerInvariant() switch
	{
		"int"      => OleDbType.Integer,
		"guid"     => OleDbType.Guid,
		"varchar"  => OleDbType.VarChar,
		"datetime" => OleDbType.Date,
		_ => throw new NotSupportedException($"Unsupported data type: {dataType}"),
	};

	/// <summary>
	/// Returns a sample value for a given <paramref name="dataType"/> identifier.
	/// </summary>
	private static object GetFieldValue(string dataType) => dataType.ToLowerInvariant() switch
	{
		"int"      => 42,
		"guid"     => Guid.NewGuid(),
		"varchar"  => "sample",
		"datetime" => DateTime.Now,
		_ => throw new NotSupportedException($"Unsupported data type: {dataType}"),
	};

	/// <summary>
	/// Creates the test table with a string Id field and all fields defined in
	/// <see cref="FieldList"/>, then creates an index on each field.
	/// </summary>
	private static void CreateTable(OleDbConnection connection)
	{
		var columns = new System.Text.StringBuilder();
		// TableName and IdFieldName are compile-time constants, not user input.
		columns.Append($"{Q(IdFieldName)} VARCHAR(255) NOT NULL PRIMARY KEY");
		foreach (var (fieldName, dataType) in FieldList)
			columns.Append($", {Q(fieldName)} {GetSqlType(dataType)}");

		string sql = $"CREATE TABLE {Q(TableName)} ({columns})";
		Console.WriteLine($"Creating table: {sql}");

		using var cmd = new OleDbCommand(sql, connection);
		cmd.ExecuteNonQuery();
		Console.WriteLine("Table created.");

		sql = $"CREATE INDEX {Q("idx_" + IdFieldName)} ON {Q(TableName)} ({Q(IdFieldName)})";
		using var cmdIdx = new OleDbCommand(sql, connection);
		cmdIdx.ExecuteNonQuery();

		foreach (var (fieldName, _) in FieldList)
		{
			sql = $"CREATE INDEX {Q("idx_" + fieldName)} ON {Q(TableName)} ({Q(fieldName)})";
			using var cmdIdxField = new OleDbCommand(sql, connection);
			cmdIdxField.ExecuteNonQuery();
		}

		Console.WriteLine("Indexes created.");
	}

	private static bool RecordExists(OleDbConnection connection, string id)
	{
		string sql = $"SELECT 1 FROM {Q(TableName)} WHERE {Q(IdFieldName)} = ?";
		using var cmd = new OleDbCommand(sql, connection);
		cmd.Parameters.AddWithValue($"@{IdFieldName}", id);

		var result = cmd.ExecuteScalar();
		if (result is int intResult)
			return intResult == 1;

		return false;
	}

	/// <summary>
	/// Inserts a new record with the given id and sample values for all fields in
	/// <see cref="FieldList"/>.
	/// </summary>
	private static void InsertRecord(OleDbConnection connection, string id)
	{
		string fields = $"{Q(IdFieldName)}, {string.Join(", ", FieldList.Select(f => Q(f.FieldName)))}";
		string values = $"?, {string.Join(", ", FieldList.Select(_ => "?"))}";

		string sql = $"INSERT INTO {Q(TableName)} ({fields}) VALUES ({values})";
		using var cmd = new OleDbCommand(sql, connection);

		// OleDb uses positional parameters (?); parameter names are metadata only.
		cmd.Parameters.AddWithValue($"@{IdFieldName}", id);
		foreach (var (fieldName, dataType) in FieldList)
			cmd.Parameters.Add($"@{fieldName}", GetOleDbType(dataType)).Value = GetFieldValue(dataType);

		cmd.ExecuteNonQuery();
	}

	/// <summary>
	/// Selects the record with the given id and reads all fields.
	/// </summary>
	private static void SelectRecord(OleDbConnection connection, string id)
	{
		string fieldList = $"{Q(IdFieldName)}, {string.Join(", ", FieldList.Select(f => Q(f.FieldName)))}";
		string sql = $"SELECT {fieldList} FROM {Q(TableName)} WHERE {Q(IdFieldName)} = ?";
		using var cmd = new OleDbCommand(sql, connection);
		cmd.Parameters.AddWithValue($"@{IdFieldName}", id);

		using var reader = cmd.ExecuteReader();
		if (reader.Read())
		{
			// Read the id field (index 0)
			_ = reader.GetString(0);

			// Read all fields from FieldList (indices 1..FieldList.Count)
			for (int i = 0; i < FieldList.Count; i++)
			{
				if (!reader.IsDBNull(i + 1))
					_ = reader.GetValue(i + 1);
			}
		}
	}

	/// <summary>
	/// Updates all fields in <see cref="FieldList"/> for the record with the given id.
	/// </summary>
	private static void UpdateRecord(OleDbConnection connection, string id)
	{
		string setClause = string.Join(", ", FieldList.Select(f => $"{Q(f.FieldName)} = ?"));

		string sql = $"UPDATE {Q(TableName)} SET {setClause} WHERE {Q(IdFieldName)} = ?";
		using var cmd = new OleDbCommand(sql, connection);

		// OleDb uses positional parameters (?); parameter names are metadata only.
		foreach (var (fieldName, dataType) in FieldList)
			cmd.Parameters.Add($"@{fieldName}", GetOleDbType(dataType)).Value = GetFieldValue(dataType);
		cmd.Parameters.AddWithValue($"@{IdFieldName}", id);

		cmd.ExecuteNonQuery();
	}
}
