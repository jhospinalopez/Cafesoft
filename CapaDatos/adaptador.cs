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
    public class DataAdapter
    {
        public CapaDatos.Connection conexion;
        public SqlDataAdapter dasql;
        public OleDbDataAdapter daole;
        public OdbcDataAdapter daodbc;
        public Npgsql.NpgsqlDataAdapter dapg;
        public MySql.Data.MySqlClient.MySqlDataAdapter dadb;

        public DataAdapter()
        {


        }

        public DataAdapter(string psql, Connection pconexion)
        {
            conexion = pconexion;
            if (pconexion.motor == "SQL")
            {
                dasql = new SqlDataAdapter(transformsql(psql), pconexion.conexionsql);
            }
            else
                if (pconexion.motor == "OLE")
                {
                    daole = new OleDbDataAdapter(transformsql(psql), pconexion.conexionole);
                }
                else
                    if (pconexion.motor == "ODBC")
                    {
                        daodbc = new OdbcDataAdapter(transformsql(psql), pconexion.conexionodbc);
                        
                    }
                    else
                        if (pconexion.motor == "PG")
                        {
                            dapg = new  Npgsql.NpgsqlDataAdapter(transformsql(psql), pconexion.conexionpg);

                        }
            else
                        if (pconexion.motor == "MY")
            {
                dadb = new MySql.Data.MySqlClient.MySqlDataAdapter(transformsql(psql),(MySql.Data.MySqlClient.MySqlConnection) pconexion.conexiondb);

            }


            SelectCommand = new Command(transformsql(psql), pconexion);
            //SelectCommand.CommandText = transformsql(psql);
        }

        public string transformsql(string psql)
        {

            string sentenciasql = psql;
            if (conexion.motor != "SQL")
            {
                sentenciasql = sentenciasql.Replace("set dateformat ymd", "");
                sentenciasql = sentenciasql.Replace("with(nolock)", "");
                sentenciasql = sentenciasql.Replace("with(paglock)", "");
                sentenciasql = sentenciasql.Replace("with(rowlock)", "");
            }

            return (sentenciasql);

        }


        public void Fill(DataSet pdata)
        {
            if (conexion.motor == "SQL")
                dasql.Fill(pdata);
            else
                if (conexion.motor == "OLE")
                    daole.Fill(pdata);
                else
                    if (conexion.motor == "ODBC")
                        daodbc.Fill(pdata);
                    else
                        if (conexion.motor == "PG")
                            dapg.Fill(pdata);
            else
                        if (conexion.motor == "MY")
                dadb.Fill(pdata);


        }

        public void Fill(DataSet pdata,string ptabla)
        {
            if (conexion.motor == "SQL")
                dasql.Fill(pdata,ptabla);
            else
                if (conexion.motor == "OLE")
                    daole.Fill(pdata,ptabla);
                else
                    if (conexion.motor == "ODBC")
                        daodbc.Fill(pdata,ptabla);
                    else
                        if (conexion.motor == "PG")
                            dapg.Fill(pdata, ptabla);
            else
                        if (conexion.motor == "MY")
                dadb.Fill(pdata, ptabla);

        }

        public void Update(DataSet ds)
        {
            if (conexion.motor == "SQL")
                dasql.Update(ds);
            else
                if (conexion.motor == "OLE")
                    daole.Update(ds);
                else
                    if (conexion.motor == "ODBC")
                        daodbc.Update(ds);
                    else
                        if (conexion.motor == "PG")
                            dapg.Update(ds);
            else
                        if (conexion.motor == "MY")
                dadb.Update(ds);


        }

        public void Update(DataSet ds,string ptabla)
        {
            if (conexion.motor == "SQL")
                dasql.Update(ds,ptabla);
            else
                if (conexion.motor == "OLE")
                    daole.Update(ds,ptabla);
                else
                    if (conexion.motor == "ODBC")
                        daodbc.Update(ds,ptabla);
                    else
                        if (conexion.motor == "PG")
                            dapg.Update(ds, ptabla);
            else
                        if (conexion.motor == "MY")
                dadb.Update(ds, ptabla);


        }



        public void estransaccion(Transaction ptrans)
        {
            if (conexion.motor == "SQL")
                dasql.SelectCommand.Transaction = ptrans.transql;
            else
                if (conexion.motor == "OLE")
                    daole.SelectCommand.Transaction = ptrans.transole;
                else
                    if (conexion.motor == "ODBC")
                        daodbc.SelectCommand.Transaction = ptrans.tranodbc;
                    else
                        if (conexion.motor == "PG")
                            dapg.SelectCommand.Transaction = ptrans.transpg;
            else
                        if (conexion.motor == "MY")
                dadb.SelectCommand.Transaction =(MySql.Data.MySqlClient.MySqlTransaction) ptrans.transdb;


        }

        public Command  SelectCommand
        {
            get
            {
                
                Command comando=null;

                if (conexion.motor == "SQL")
                {
                    if (this.dasql.SelectCommand ==null)
                        comando =new Command("select 1",conexion);
                    else
                        comando =new Command(dasql.SelectCommand.CommandText,conexion);

                    dasql.SelectCommand = comando.cmdsql;
                }
                else
                    if (conexion.motor == "OLE")
                    {
                        if (this.daole.SelectCommand == null)
                            comando = new Command("select 1", conexion);
                        else
                            comando = new Command(daole.SelectCommand.CommandText, conexion);

                        daole.SelectCommand = comando.cmdole;

                    }
                    else
                        if (conexion.motor == "ODBC")
                        {
                            if (this.daodbc.SelectCommand == null)
                                comando = new Command("select 1", conexion);
                            else
                                comando = new Command(daodbc.SelectCommand.CommandText, conexion);

                            daodbc.SelectCommand = comando.cmdodbc;

                        }
                        else
                            if (conexion.motor == "PG")
                            {
                                if (this.dapg.SelectCommand == null)
                                    comando = new Command("select 1", conexion);
                                else
                                    comando = new Command(dapg.SelectCommand.CommandText, conexion);

                                dapg.SelectCommand = comando.cmdpg;

                            }
                else
                            if (conexion.motor == "MY")
                {
                    if (this.dadb.SelectCommand == null)
                        comando = new Command("select 1", conexion);
                    else
                        comando = new Command(dadb.SelectCommand.CommandText, conexion);

                    dadb.SelectCommand = comando.cmddb;

                }

                return (comando);

            }

            set
            {
                if (conexion.motor == "SQL")
                    dasql.SelectCommand = value.cmdsql;
                else
                    if (conexion.motor == "OLE")
                    daole.SelectCommand = value.cmdole;
                else
                        if (conexion.motor == "ODBC")
                    daodbc.SelectCommand = value.cmdodbc;
                else
                            if (conexion.motor == "PG")
                    dapg.SelectCommand = value.cmdpg;
                else
                            if (conexion.motor == "MY")
                    dadb.SelectCommand = value.cmddb;

            }

        }


        public Command InsertCommand
        {
            get
            {
                Command comando = null;

                if (conexion.motor == "SQL")
                {
                    if (this.dasql.InsertCommand == null)
                        comando = new Command("select 1", conexion);
                    else
                        comando = new Command(dasql.InsertCommand.CommandText, conexion);

                    dasql.InsertCommand = comando.cmdsql;
                }
                else
                    if (conexion.motor == "OLE")
                    {
                        if (this.daole.InsertCommand == null)
                            comando = new Command("select 1", conexion);
                        else
                            comando = new Command(daole.InsertCommand.CommandText, conexion);

                        daole.InsertCommand = comando.cmdole;

                    }
                    else
                        if (conexion.motor == "ODBC")
                        {
                            if (this.daodbc.InsertCommand == null)
                                comando = new Command("select 1", conexion);
                            else
                                comando = new Command(daodbc.InsertCommand.CommandText, conexion);

                            daodbc.InsertCommand = comando.cmdodbc;

                        }
                        else
                            if (conexion.motor == "PG")
                            {
                                if (this.dapg.InsertCommand == null)
                                    comando = new Command("select 1", conexion);
                                else
                                    comando = new Command(dapg.InsertCommand.CommandText, conexion);

                                dapg.InsertCommand = comando.cmdpg;

                            }
                else
                            if (conexion.motor == "MY")
                {
                    if (this.dadb.InsertCommand == null)
                        comando = new Command("select 1", conexion);
                    else
                        comando = new Command(dadb.InsertCommand.CommandText, conexion);

                    dadb.InsertCommand = comando.cmddb;

                }


                return (comando);

            }

            set
            {
                if (conexion.motor == "SQL")
                    dasql.InsertCommand = value.cmdsql;
                else
                    if (conexion.motor == "OLE")
                        daole.InsertCommand = value.cmdole;
                    else
                        if (conexion.motor == "ODBC")
                            daodbc.InsertCommand = value.cmdodbc;
                        else
                            if (conexion.motor == "PG")
                                dapg.InsertCommand = value.cmdpg;
                else
                            if (conexion.motor == "MY")
                    dadb.InsertCommand = value.cmddb;


            }

        }


        public Command UpdateCommand
        {
            get
            {
                Command comando = null;

                if (conexion.motor == "SQL")
                {
                    if (this.dasql.UpdateCommand == null)
                        comando = new Command("select 1", conexion);
                    else
                        comando = new Command(dasql.UpdateCommand.CommandText, conexion);

                    dasql.UpdateCommand = comando.cmdsql;
                }
                else
                    if (conexion.motor == "OLE")
                    {
                        if (this.daole.UpdateCommand == null)
                            comando = new Command("select 1", conexion);
                        else
                            comando = new Command(daole.UpdateCommand.CommandText, conexion);

                        daole.UpdateCommand = comando.cmdole;

                    }
                    else
                        if (conexion.motor == "ODBC")
                        {
                            if (this.daodbc.UpdateCommand == null)
                                comando = new Command("select 1", conexion);
                            else
                                comando = new Command(daodbc.UpdateCommand.CommandText, conexion);

                            daodbc.UpdateCommand = comando.cmdodbc;

                        }
                        else
                            if (conexion.motor == "PG")
                            {
                                if (this.dapg.UpdateCommand == null)
                                    comando = new Command("select 1", conexion);
                                else
                                    comando = new Command(dapg.UpdateCommand.CommandText, conexion);

                                dapg.UpdateCommand = comando.cmdpg;

                            }
                else
                            if (conexion.motor == "MY")
                {
                    if (this.dadb.UpdateCommand == null)
                        comando = new Command("select 1", conexion);
                    else
                        comando = new Command(dadb.UpdateCommand.CommandText, conexion);

                    dadb.UpdateCommand = comando.cmddb;

                }


                return (comando);

            }

            set
            {
                if (conexion.motor == "SQL")
                    dasql.UpdateCommand = value.cmdsql;
                else
                    if (conexion.motor == "OLE")
                        daole.UpdateCommand = value.cmdole;
                    else
                        if (conexion.motor == "ODBC")
                            daodbc.UpdateCommand = value.cmdodbc;
                        else
                            if (conexion.motor == "PG")
                                dapg.UpdateCommand = value.cmdpg;
                else
                            if (conexion.motor == "MY")
                    dadb.UpdateCommand = value.cmddb;


            }

        }


        public Command DeleteCommand
        {
            get
            {
                Command comando = null;

                if (conexion.motor == "SQL")
                {
                    if (this.dasql.DeleteCommand == null)
                        comando = new Command("select 1", conexion);
                    else
                        comando = new Command(dasql.DeleteCommand.CommandText, conexion);

                    dasql.DeleteCommand = comando.cmdsql;
                }
                else
                    if (conexion.motor == "OLE")
                    {
                        if (this.daole.DeleteCommand == null)
                            comando = new Command("select 1", conexion);
                        else
                            comando = new Command(daole.DeleteCommand.CommandText, conexion);

                        daole.DeleteCommand = comando.cmdole;

                    }
                    else
                        if (conexion.motor == "ODBC")
                        {
                            if (this.daodbc.DeleteCommand == null)
                                comando = new Command("select 1", conexion);
                            else
                                comando = new Command(daodbc.DeleteCommand.CommandText, conexion);

                            daodbc.DeleteCommand = comando.cmdodbc;

                        }
                        else
                            if (conexion.motor == "PG")
                            {
                                if (this.dapg.DeleteCommand == null)
                                    comando = new Command("select 1", conexion);
                                else
                                    comando = new Command(dapg.DeleteCommand.CommandText, conexion);

                                dapg.DeleteCommand = comando.cmdpg;

                            }
                else
                            if (conexion.motor == "MY")
                {
                    if (this.dadb.DeleteCommand == null)
                        comando = new Command("select 1", conexion);
                    else
                        comando = new Command(dadb.DeleteCommand.CommandText, conexion);

                    dadb.DeleteCommand = comando.cmddb;

                }

                return (comando);

            }

            set
            {
                if (conexion.motor == "SQL")
                    dasql.DeleteCommand = value.cmdsql;
                else
                    if (conexion.motor == "OLE")
                        daole.DeleteCommand = value.cmdole;
                    else
                        if (conexion.motor == "ODBC")
                            daodbc.DeleteCommand = value.cmdodbc;
                        else
                            if (conexion.motor == "PG")
                                dapg.DeleteCommand = value.cmdpg;
                else
                            if (conexion.motor == "MY")
                    dadb.DeleteCommand = value.cmddb;


            }

        }

        public void SetInsert(CommandBuilder pcb)
        {
            if (conexion.motor == "SQL")
                this.dasql.InsertCommand = pcb.cbsql.GetInsertCommand();
            else
                if (conexion.motor == "OLE")
                    this.daole.InsertCommand = pcb.cbole.GetInsertCommand();
                else
            if (conexion.motor == "ODBC")
                this.daodbc.InsertCommand = pcb.cbodbc.GetInsertCommand();
            else
                if (conexion.motor == "PG")
                    this.dapg.InsertCommand = pcb.cbpg.GetInsertCommand();
            else
                if (conexion.motor == "MY")
                this.dadb.InsertCommand = pcb.cbdb.GetInsertCommand();

        }

        public void SetUpdate(CommandBuilder pcb)
        {
            if (conexion.motor == "SQL")
                this.dasql.UpdateCommand = pcb.cbsql.GetUpdateCommand();
            else
                if (conexion.motor == "OLE")
                    this.daole.UpdateCommand = pcb.cbole.GetUpdateCommand();
                else
                    if (conexion.motor == "ODBC")
                        this.daodbc.UpdateCommand = pcb.cbodbc.GetUpdateCommand();
                    else
                        if (conexion.motor == "PG")
                            this.dapg.UpdateCommand = pcb.cbpg.GetUpdateCommand();
            else
                        if (conexion.motor == "MY")
                this.dadb.UpdateCommand = pcb.cbdb.GetUpdateCommand();

        }

        public void SetDelete(CommandBuilder pcb)
        {
            if (conexion.motor == "SQL")
                this.dasql.DeleteCommand = pcb.cbsql.GetDeleteCommand();
            else
                if (conexion.motor == "OLE")
                    this.daole.DeleteCommand = pcb.cbole.GetDeleteCommand();
                else
                    if (conexion.motor == "ODBC")
                        this.daodbc.DeleteCommand = pcb.cbodbc.GetDeleteCommand();
                    else
                        if (conexion.motor == "PG")
                            this.dapg.DeleteCommand = pcb.cbpg.GetDeleteCommand();
            else
                        if (conexion.motor == "MY")
                this.dadb.DeleteCommand = pcb.cbdb.GetDeleteCommand();


        }

        public IDataReader ExecuteReader()
        {

            if (conexion.motor == "SQL")
                return ((SqlDataReader)this.dasql.SelectCommand.ExecuteReader());
            else
                if (conexion.motor == "PG")
                return ((Npgsql.NpgsqlDataReader)this.dapg.SelectCommand.ExecuteReader());
            else
                if (conexion.motor =="MY")
                return ((MySql.Data.MySqlClient.MySqlDataReader)this.dadb.SelectCommand.ExecuteReader());
            else
                if (conexion.motor == "ODBC")
                return ((OdbcDataReader)this.daodbc.SelectCommand.ExecuteReader());
            else
                if (conexion.motor == "OLE")
                return ((OleDbDataReader)this.daole.SelectCommand.ExecuteReader());

            return (null);


        }


    }
}
