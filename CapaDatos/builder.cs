using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.SqlClient;
using System.Data.Odbc;
using System.Data.OleDb;


namespace CapaDatos
{
    public class CommandBuilder
    {
        
        public SqlCommandBuilder cbsql;
        public OleDbCommandBuilder cbole;
        public OdbcCommandBuilder cbodbc;
        public Npgsql.NpgsqlCommandBuilder cbpg;
        public  MySql.Data.MySqlClient.MySqlCommandBuilder cbdb;

        public CommandBuilder(ref DataAdapter da)
        {

            if (da.conexion.motor == "SQL")
            {
                cbsql = new SqlCommandBuilder(da.dasql);
                //da.dasql.UpdateCommand = cbsql.GetUpdateCommand();
                //da.dasql.DeleteCommand = cbsql.GetDeleteCommand();
                //da.dasql.InsertCommand = cbsql.GetInsertCommand();
            }
            else
                if (da.conexion.motor == "OLE")
                {
                    cbole = new OleDbCommandBuilder(da.daole);
                    //da.daole.UpdateCommand = cbole.GetUpdateCommand();
                    //da.daole.DeleteCommand = cbole.GetDeleteCommand();
                    //da.daole.InsertCommand = cbole.GetInsertCommand();

                }
                else
                    if (da.conexion.motor == "ODBC")
                    {
                        cbodbc = new OdbcCommandBuilder(da.daodbc);
                        //da.daodbc.UpdateCommand = cbodbc.GetUpdateCommand();
                        //da.daodbc.DeleteCommand = cbodbc.GetDeleteCommand();
                        //da.daodbc.InsertCommand = cbodbc.GetInsertCommand();

                    }
                    else
                        if (da.conexion.motor == "PG")
                        {
                            cbpg = new  Npgsql.NpgsqlCommandBuilder(da.dapg);
                            
                            //da.dapg.UpdateCommand = cbpg.GetUpdateCommand();
                            //da.dapg.DeleteCommand = cbpg.GetDeleteCommand();
                            //da.dapg.InsertCommand = cbpg.GetInsertCommand();

                        }
            else
                        if (da.conexion.motor == "MY")
            {
                cbdb = new MySql.Data.MySqlClient.MySqlCommandBuilder(da.dadb);

                //da.dapg.UpdateCommand = cbpg.GetUpdateCommand();
                //da.dapg.DeleteCommand = cbpg.GetDeleteCommand();
                //da.dapg.InsertCommand = cbpg.GetInsertCommand();

            }


        }


    }    
}
