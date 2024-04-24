using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SharpDatabase
{
    public static class Database
    {
        private static MySqlConnection connection;
        public static DatabaseLine ExecuteCommand(string sqlCommand)
        {

            try
            {
                connection.Open();

                MySqlCommand command = new MySqlCommand(sqlCommand, connection);
                using (MySqlDataReader reader = command.ExecuteReader())
                {
                    if (!reader.HasRows)
                    {
                        return new DatabaseLine(null, null);
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

                    return new DatabaseLine(columnNames.ToArray(), columnValues.ToArray());
                }
            }
            catch (Exception ex)
            {

                Console.WriteLine(ex.Message);
                return new DatabaseLine(null, null);
            }
            finally
            {
                if (connection.State == System.Data.ConnectionState.Open)
                {
                    connection.Close();
                }
            }
        }
        public static void ConnectToDataBase(string database, string server = "127.0.0.1", string userId = "root", string password = "")
        {

            try
            {
                string connectionString = $"server={server};user id={userId};password={password};database={database}";
                MySqlConnection conn = new MySqlConnection(connectionString);
                connection = conn;
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }


    }

    public class DatabaseLine
    {

        private string[] columName;
        private string[] columValue;

        public DatabaseLine(string[] columName, string[] columValue)
        {
            this.columName = columName;
            this.columValue = columValue;
        }

        public bool Read()
        {
            return columName != null && columValue != null;
        }

        public string GetValue(string nameTag)
        {
            for (int i = 0; i < columName.Length; i++)
            {
                if (columName[i] == nameTag)
                {
                    return columValue[i];
                }
            }

            return "";
        }

        public int GetCountTag()
        {
            return columName.Length;
        }

        public string GetValueFromIndex(int index)
        {
            return columValue[index];
        }

        public string GetTagFromIndex(int index)
        {
            return columName[index];
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

                DatabaseLine newLine = new DatabaseLine(columName.ToArray(), columValue.ToArray());
                lines.Add(newLine);
            }

            return lines.ToArray();
        }
    }
}
