using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlClient;
using System.Threading.Tasks;

namespace SharpDatabase
{
    public static class Database
    {
        private static string _connectionString;

        public static void ConnectToDataBase(string database, string server = "127.0.0.1", string userId = "root", string password = "")
        {
            _connectionString = $"server={server};user id={userId};password={password};database={database}";
        }

        //-------------------------------------------------------------

        public static DatabaseLine ExecuteCommand(string sqlCommand)
        {
            using (MySqlConnection connection = new MySqlConnection(_connectionString))
            {
                try
                {
                    connection.Open();

                    MySqlCommand command = new MySqlCommand(sqlCommand, connection);
                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (!reader.HasRows)
                        {
                            return new DatabaseLine(null, null, connection.BeginTransaction());
                        }

                        List<string> columnNames = new List<string>();
                        List<string> columnValues = new List<string>();

                        while (reader.Read())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                columnNames.Add(reader.GetName(i));
                                columnValues.Add(reader[i].ToString());
                            }
                        }

                        return new DatabaseLine(columnNames.ToArray(), columnValues.ToArray(), connection.BeginTransaction());
                    }
                }
                catch (Exception ex)
                {
                    throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
                }
            }
        }

        public static DatabaseLine ExecuteCommand(MySqlConnection connection,string sqlCommand)
        {
            try
            {
                connection.Open();

                MySqlCommand command = new MySqlCommand(sqlCommand, connection);
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (!reader.HasRows)
                    {
                        return new DatabaseLine(null, null, connection.BeginTransaction());
                    }

                    List<string> columnNames = new List<string>();
                    List<string> columnValues = new List<string>();

                    while (reader.Read())
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            columnNames.Add(reader.GetName(i));
                            columnValues.Add(reader[i].ToString());
                        }
                    }

                    return new DatabaseLine(columnNames.ToArray(), columnValues.ToArray(), connection.BeginTransaction());
                }
            }
            catch (Exception ex)
            {
                throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
            }
            finally
            {
                connection.Close();
            }
        }

        public static DatabaseLine ExecuteCommand(MySqlConnection connection, MySqlCommand command)
        {
            try
            {
                connection.Open();
                command.Connection = connection;

                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (!reader.HasRows)
                    {
                        return new DatabaseLine(null, null, connection.BeginTransaction());
                    }

                    List<string> columnNames = new List<string>();
                    List<string> columnValues = new List<string>();

                    while (reader.Read())
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            columnNames.Add(reader.GetName(i));
                            columnValues.Add(reader[i].ToString());
                        }
                    }

                    return new DatabaseLine(columnNames.ToArray(), columnValues.ToArray(), connection.BeginTransaction());
                }
            }
            catch (Exception ex)
            {
                throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
            }
            finally
            {
                connection.Close();
            }
        }

        public static DatabaseLine ExecuteCommand(MySqlCommand command)
        {
            using (MySqlConnection connection = new MySqlConnection(_connectionString))
            {
                try
                {
                    connection.Open();
                    command.Connection = connection;

                    using (MySqlDataReader reader = command.ExecuteReader())
                    {
                        if (!reader.HasRows)
                        {
                            return new DatabaseLine(null, null, connection.BeginTransaction());
                        }

                        List<string> columnNames = new List<string>();
                        List<string> columnValues = new List<string>();

                        while (reader.Read())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                columnNames.Add(reader.GetName(i));
                                columnValues.Add(reader[i].ToString());
                            }
                        }

                        return new DatabaseLine(columnNames.ToArray(), columnValues.ToArray(), connection.BeginTransaction());
                    }
                }
                catch (Exception ex)
                {
                    throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
                }
            }
        }

        //-------------------------------------------------------------

        public static async Task<DatabaseLine> ExecuteCommandAsync(string sqlCommand)
        {
            using (MySqlConnection connection = new MySqlConnection(_connectionString))
            {
                try
                {
                    await connection.OpenAsync();

                    using (MySqlCommand command = new MySqlCommand(sqlCommand, connection))
                    using (DbDataReader reader = await command.ExecuteReaderAsync())
                    {
                        if (!reader.HasRows)
                        {
                            return new DatabaseLine(null, null, connection.BeginTransaction());
                        }

                        List<string> columnNames = new List<string>();
                        List<string> columnValues = new List<string>();

                        while (await reader.ReadAsync())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                columnNames.Add(reader.GetName(i));
                                columnValues.Add(reader[i].ToString());
                            }
                        }

                        return new DatabaseLine(columnNames.ToArray(), columnValues.ToArray(), connection.BeginTransaction());
                    }
                }
                catch(Exception ex)
                {
                    throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
                }
            }
        }

        public static async Task<DatabaseLine> ExecuteCommandAsync(MySqlCommand command)
        {
            using (MySqlConnection connection = new MySqlConnection(_connectionString))
            {
                try
                {
                    await connection.OpenAsync();
                    command.Connection = connection;

                    using (DbDataReader reader = await command.ExecuteReaderAsync())
                    {
                        if (!reader.HasRows)
                        {
                            return new DatabaseLine(null, null, connection.BeginTransaction());
                        }

                        List<string> columnNames = new List<string>();
                        List<string> columnValues = new List<string>();

                        while (await reader.ReadAsync())
                        {
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                columnNames.Add(reader.GetName(i));
                                columnValues.Add(reader[i].ToString());
                            }
                        }

                        return new DatabaseLine(columnNames.ToArray(), columnValues.ToArray(), connection.BeginTransaction());
                    }
                }
                catch (Exception ex)
                {
                    throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
                }
            }
        }

        public static async Task<DatabaseLine> ExecuteCommandAsync(MySqlConnection connection, MySqlCommand command)
        {
            try
            {
                await connection.OpenAsync();
                command.Connection = connection;

                using (DbDataReader reader = await command.ExecuteReaderAsync())
                {
                    if (!reader.HasRows)
                    {
                        return new DatabaseLine(null, null, connection.BeginTransaction());
                    }

                    List<string> columnNames = new List<string>();
                    List<string> columnValues = new List<string>();

                    while (await reader.ReadAsync())
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            columnNames.Add(reader.GetName(i));
                            columnValues.Add(reader[i].ToString());
                        }
                    }

                    return new DatabaseLine(columnNames.ToArray(), columnValues.ToArray(), connection.BeginTransaction());
                }
            }
            catch (Exception ex)
            {
                throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        public static async Task<DatabaseLine> ExecuteCommandAsync(MySqlConnection connection,string sqlCommand)
        {
            try
            {
                await connection.OpenAsync();

                using (MySqlCommand command = new MySqlCommand(sqlCommand, connection))
                using (DbDataReader reader = await command.ExecuteReaderAsync())
                {
                    if (!reader.HasRows)
                    {
                        return new DatabaseLine(null, null, connection.BeginTransaction());
                    }

                    List<string> columnNames = new List<string>();
                    List<string> columnValues = new List<string>();

                    while (await reader.ReadAsync())
                    {
                        for (int i = 0; i < reader.FieldCount; i++)
                        {
                            columnNames.Add(reader.GetName(i));
                            columnValues.Add(reader[i].ToString());
                        }
                    }

                    return new DatabaseLine(columnNames.ToArray(), columnValues.ToArray(), connection.BeginTransaction());
                }
            }
            catch (Exception ex)
            {
                throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        //--------------------------------------------------------------

        public static bool DoesLineExist(string tableName, string key, string value)
        {
            string sqlCommand = $"SELECT EXISTS(SELECT 1 FROM {tableName} WHERE {key} = @value)";

            using (MySqlConnection connection = new MySqlConnection(_connectionString))
            {
                try
                {
                    connection.Open();

                    using (MySqlCommand command = new MySqlCommand(sqlCommand, connection))
                    {
                        command.Parameters.AddWithValue("@value", value);

                        object result = command.ExecuteScalar();
                        return Convert.ToBoolean(result);
                    }
                }
                catch (Exception ex)
                {
                    throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
                }
            }
        }

        public static bool DoesLineExist(MySqlConnection connection, string tableName, string key, string value)
        {
            string sqlCommand = $"SELECT EXISTS(SELECT 1 FROM {tableName} WHERE {key} = @value)";

            try
            {
                connection.Open();

                using (MySqlCommand command = new MySqlCommand(sqlCommand, connection))
                {
                    command.Parameters.AddWithValue("@value", value);

                    object result = command.ExecuteScalar();
                    return Convert.ToBoolean(result);
                }
            }
            catch (Exception ex)
            {
                throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
            }
            finally
            {
                connection.Close();
            }
        }

        //--------------------------------------------------------------

        public static async Task<bool> DoesLineExistAsync(string tableName, string key, string value)
        {
            string sqlCommand = $"SELECT EXISTS(SELECT 1 FROM {tableName} WHERE {key} = @value)";

            using (MySqlConnection connection = new MySqlConnection(_connectionString))
            {
                try
                {
                    await connection.OpenAsync();

                    using (MySqlCommand command = new MySqlCommand(sqlCommand, connection))
                    {
                        command.Parameters.AddWithValue("@value", value);

                        object result = await command.ExecuteScalarAsync();
                        return Convert.ToBoolean(result);
                    }
                }
                catch(Exception ex)
                {
                    throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
                }
            }
        }

        public static async Task<bool> DoesLineExistAsync(MySqlConnection connection,string tableName, string key, string value)
        {
            string sqlCommand = $"SELECT EXISTS(SELECT 1 FROM {tableName} WHERE {key} = @value)";

            try
            {
                await connection.OpenAsync();

                using (MySqlCommand command = new MySqlCommand(sqlCommand, connection))
                {
                    command.Parameters.AddWithValue("@value", value);

                    object result = await command.ExecuteScalarAsync();
                    return Convert.ToBoolean(result);
                }
            }
            catch (Exception ex)
            {
                throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        //--------------------------------------------------------------

        public static async Task<bool> UpdateRecordAsync(string tableName, string conditionColumn, string conditionValue, string updateColumn, string updateValue)
        {
            string sqlCommand = $"UPDATE {tableName} SET {updateColumn} = @updateValue WHERE {conditionColumn} = @conditionValue";

            using (MySqlConnection connection = new MySqlConnection(_connectionString))
            {
                try
                {
                    await connection.OpenAsync();

                    using (MySqlCommand command = new MySqlCommand(sqlCommand, connection))
                    {
                        command.Parameters.AddWithValue("@updateValue", updateValue);
                        command.Parameters.AddWithValue("@conditionValue", conditionValue);

                        int affectedRows = await command.ExecuteNonQueryAsync();
                        return affectedRows > 0;
                    }
                }
                catch(Exception ex)
                {
                    throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
                }
            }
        }

        public static async Task<bool> UpdateRecordAsync(MySqlConnection connection,string tableName, string conditionColumn, string conditionValue, string updateColumn, string updateValue)
        {
            string sqlCommand = $"UPDATE {tableName} SET {updateColumn} = @updateValue WHERE {conditionColumn} = @conditionValue";

            try
            {
                await connection.OpenAsync();

                using (MySqlCommand command = new MySqlCommand(sqlCommand, connection))
                {
                    command.Parameters.AddWithValue("@updateValue", updateValue);
                    command.Parameters.AddWithValue("@conditionValue", conditionValue);

                    int affectedRows = await command.ExecuteNonQueryAsync();
                    return affectedRows > 0;
                }
            }
            catch (Exception ex)
            {
                throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        //--------------------------------------------------------------

        public static List<DatabaseLine> ExecuteOperationsWithSharedConnection(params Func<MySqlConnection, DatabaseLine>[] operations)
        {
            var results = new List<DatabaseLine>();

            using (var conn = new MySqlConnection(_connectionString))
            {
                conn.Open();

                foreach (var operation in operations)
                {
                    results.Add(operation(conn));
                }

                return results;

            }
        }

        //--------------------------------------------------------------

        public static async Task<DatabaseLine> ExecuteParameterizedCommandAsync(string query, Dictionary<string, object> parameters)
        {
            using (MySqlConnection connection = new MySqlConnection(_connectionString))
            {
                try
                {
                    using (MySqlCommand command = new MySqlCommand(query, connection))
                    {
                        foreach (var param in parameters)
                        {
                            command.Parameters.AddWithValue(param.Key, param.Value ?? DBNull.Value);
                        }

                        await connection.OpenAsync();

                        using (var reader = await command.ExecuteReaderAsync() as MySqlDataReader)
                        {
                            if (await reader.ReadAsync())
                            {
                                List<string> columnNames = new List<string>();
                                List<string> columnValues = new List<string>();

                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    columnNames.Add(reader.GetName(i));
                                    columnValues.Add(reader[i].ToString());
                                }
                                return new DatabaseLine(columnNames.ToArray(), columnValues.ToArray(), connection.BeginTransaction());
                            }
                            else
                            {
                                return new DatabaseLine(null, null, connection.BeginTransaction());
                            }
                        }
                    }
                }
                catch(Exception e)
                {
                    throw new ExceptionDatabase(connection.BeginTransaction(), e.Message);
                }
            }
        }

        public static async Task<DatabaseLine> ExecuteParameterizedCommandAsync(MySqlConnection connection, string query, Dictionary<string, object> parameters)
        {
            try
            {
                using (MySqlCommand command = new MySqlCommand(query, connection))
                {
                    foreach (var param in parameters)
                    {
                        command.Parameters.AddWithValue(param.Key, param.Value ?? DBNull.Value);
                    }

                    await connection.OpenAsync();

                    using (var reader = await command.ExecuteReaderAsync() as MySqlDataReader)
                    {
                        if (await reader.ReadAsync())
                        {
                            List<string> columnNames = new List<string>();
                            List<string> columnValues = new List<string>();

                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                columnNames.Add(reader.GetName(i));
                                columnValues.Add(reader[i].ToString());
                            }
                            return new DatabaseLine(columnNames.ToArray(), columnValues.ToArray(), connection.BeginTransaction());
                        }
                        else
                        {
                            return new DatabaseLine(null, null, connection.BeginTransaction());
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new ExceptionDatabase(connection.BeginTransaction(), e.Message);
            }
            finally
            {
                await connection.CloseAsync();
            }
        }

        //--------------------------------------------------------------

        public static async Task<DatabaseRow[]> GetDatabaseRowsAsync(string query)
        {
            List<DatabaseRow> rows = new List<DatabaseRow>();
            using (var connection = new MySqlConnection(_connectionString))
            {
                try
                {
                    await connection.OpenAsync();
                    using (var command = new MySqlCommand(query, connection))
                    {
                        using (var reader = await command.ExecuteReaderAsync())
                        {
                            while (await reader.ReadAsync())
                            {
                                DatabaseRow row = new DatabaseRow();
                                for (int i = 0; i < reader.FieldCount; i++)
                                {
                                    row[reader.GetName(i)] = await reader.IsDBNullAsync(i) ? null : reader.GetValue(i);
                                }
                                rows.Add(row);
                            }
                        }
                    }
                }
                catch(Exception ex)
                {
                    throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
                }
            }
            return rows.ToArray();
        }

        public static async Task<DatabaseRow[]> GetDatabaseRowsAsync(MySqlConnection connection, string query)
        {
            List<DatabaseRow> rows = new List<DatabaseRow>();
            try
            {
                await connection.OpenAsync();
                using (var command = new MySqlCommand(query, connection))
                {
                    using (var reader = await command.ExecuteReaderAsync())
                    {
                        while (await reader.ReadAsync())
                        {
                            DatabaseRow row = new DatabaseRow();
                            for (int i = 0; i < reader.FieldCount; i++)
                            {
                                row[reader.GetName(i)] = await reader.IsDBNullAsync(i) ? null : reader.GetValue(i);
                            }
                            rows.Add(row);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ExceptionDatabase(connection.BeginTransaction(), ex.Message);
            }
            finally
            {
                await connection.CloseAsync();
            }
            return rows.ToArray();
        }

        public static string GetConnectionString()
        {
            return _connectionString;
        }
    }

    public class ExceptionDatabase : Exception
    {
        public DbTransaction Transaction;
        public ExceptionDatabase(DbTransaction tr, string message) : base(message)
        {
            Transaction = tr;
        }
    }

    public class DatabaseLine
    {
        private string[] columName;
        private string[] columValue;

        private DbTransaction transaction;

        public DatabaseLine(string[] columName, string[] columValue, DbTransaction transaction)
        {
            this.columName = columName;
            this.columValue = columValue;
            this.transaction = transaction;
        }

        public DbTransaction GetTransaction() => transaction;

        public bool Read() => columName != null && columValue != null;

        public string GetValue(string nameTag)
        {
            int index = Array.IndexOf(columName, nameTag);
            return index >= 0 ? columValue[index] : "";
        }

        public int GetCountTag() => Read() ? columName.Length : 0;

        public string GetValueFromIndex(int index)
        {
            if (index >= 0 && index < columValue.Length)
            {
                return columValue[index];
            }
            throw new IndexOutOfRangeException($"Index {index} is out of range.");
        }

        public string GetTagFromIndex(int index)
        {
            if (index >= 0 && index < columName.Length)
            {
                return columName[index];
            }
            throw new IndexOutOfRangeException($"Index {index} is out of range.");
        }

        public DatabaseLine[] Split(string database, bool ex = true)
        {
            DatabaseLine line = this;

            DatabaseLine l = Database.ExecuteCommand($"SELECT * FROM {database} LIMIT 1");

            if (line.GetCountTag() == l.GetCountTag())
            {
                if (ex)
                {
                    throw new InvalidOperationException("There is 1 line in this class");
                }
                else
                {
                    return new DatabaseLine[] { this };
                }
            }

            int count = line.GetCountTag() / l.GetCountTag();
            List<DatabaseLine> lines = new List<DatabaseLine>(count);

            for (int a = 0; a < count; a++)
            {
                List<string> columName = new List<string>();
                List<string> columValue = new List<string>();
                for (int i = a * l.GetCountTag(); i < line.GetCountTag(); i++)
                {
                    columName.Add(line.GetTagFromIndex(i));
                    columValue.Add(line.GetValueFromIndex(i));
                }

                DatabaseLine newLine = new DatabaseLine(columName.ToArray(), columValue.ToArray(), transaction);
                lines.Add(newLine);
            }

            return lines.ToArray();
        }
    }

    public class DatabaseRow
    {
        private Dictionary<string, object> _data = new Dictionary<string, object>();

        public object this[string columnName]
        {
            get { return _data[columnName]; }
            set { _data[columnName] = value; }
        }

        public T GetValue<T>(string columnName)
        {
            if (_data.TryGetValue(columnName, out object value))
            {
                return (T)value;
            }
            return default(T);
        }
    }
}
