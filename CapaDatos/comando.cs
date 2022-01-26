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
    public class Command
    {
        public Connection conexion;
        public SqlCommand cmdsql;
        public OleDbCommand cmdole;
        public OdbcCommand cmdodbc;
        public Npgsql.NpgsqlCommand cmdpg;
        public MySql.Data.MySqlClient.MySqlCommand cmddb;
        public DataSet dsParametros;
        public string sentenciasql;
       
        public Command()
        {
            dsParametros = new DataSet();
            dsParametros.Tables.Add("tabla");
            dsParametros.Tables[0].Columns.Add("parametro");
            dsParametros.Tables[0].Columns.Add("campo");
            dsParametros.Tables[0].Columns.Add("tipo");
            dsParametros.Tables[0].Columns.Add("valor");

        }


        public string transformsql(string psql)
        {

            sentenciasql = psql;
            if (conexion.motor != "SQL")
            {
                sentenciasql = sentenciasql.Replace("set dateformat ymd", "");
                sentenciasql = sentenciasql.Replace("with(nolock)", "");
                sentenciasql = sentenciasql.Replace("with(paglock)", "");
                sentenciasql = sentenciasql.Replace("with(rowlock)", "");
            }

            return (sentenciasql);

        }

        public Command(string psql, Connection pcone)
        {
            conexion = pcone;
            sentenciasql = transformsql( psql);
            if (conexion.motor == "SQL")
                cmdsql = new SqlCommand(sentenciasql, conexion.conexionsql);
            else
                if (conexion.motor == "OLE")
                    cmdole = new OleDbCommand (sentenciasql, conexion.conexionole);
                else
                    if (conexion.motor == "ODBC")
                        cmdodbc = new OdbcCommand(sentenciasql, conexion.conexionodbc);
                    else
                        if (conexion.motor == "PG")
                            cmdpg = new Npgsql.NpgsqlCommand(sentenciasql, conexion.conexionpg);
            else
                        if (conexion.motor == "MY")
                cmddb = new  MySql.Data.MySqlClient.MySqlCommand(sentenciasql,(MySql.Data.MySqlClient.MySqlConnection) conexion.conexiondb);


            dsParametros = new DataSet();
            dsParametros.Tables.Add("tabla");
            dsParametros.Tables[0].Columns.Add("parametro");
            dsParametros.Tables[0].Columns.Add("campo");
            dsParametros.Tables[0].Columns.Add("tipo");
            dsParametros.Tables[0].Columns.Add("ancho",System.Type.GetType("System.Int32"));
            dsParametros.Tables[0].Columns.Add("valor");
            
        }

        public void SelectCommand(string psql)
        {
            sentenciasql = transformsql( psql);

            if (conexion.motor == "SQL")
            {
                if (cmdsql == null)
                    cmdsql = new SqlCommand(sentenciasql,conexion.conexionsql);
                cmdsql.CommandText = sentenciasql;
            }
            else
                if (conexion.motor == "OLE")
                {
                    if (cmdole == null)
                        cmdole = new OleDbCommand(sentenciasql, conexion.conexionole);

                    cmdole.CommandText = sentenciasql;
                }
                else
                    if (conexion.motor == "ODBC")
                    {
                        if (cmdodbc == null)
                            cmdodbc = new OdbcCommand(sentenciasql, conexion.conexionodbc);

                        cmdodbc.CommandText = sentenciasql;
                    }
                    else
                        if (conexion.motor == "PG")
                        {
                            if (cmdpg == null)
                                cmdpg = new Npgsql.NpgsqlCommand(sentenciasql, conexion.conexionpg);

                            cmdodbc.CommandText = sentenciasql;
                        }
            else
                        if (conexion.motor == "MY")
            {
                if (cmddb == null)
                    cmddb = new  MySql.Data.MySqlClient.MySqlCommand(sentenciasql,(MySql.Data.MySqlClient.MySqlConnection) conexion.conexiondb);

                cmddb.CommandText = sentenciasql;
            }


        }

        public int ExecuteNonQuery()
        {

            if (conexion.motor == "SQL")
            {
                return (cmdsql.ExecuteNonQuery());
            }
            else
                if (conexion.motor == "OLE")
                    return (cmdole.ExecuteNonQuery());
                else
                    if (conexion.motor == "ODBC")
                        return (cmdodbc.ExecuteNonQuery());
                    else
                        if (conexion.motor == "PG")
                            return (cmdpg.ExecuteNonQuery());
            else
                        if (conexion.motor == "MY")
                return (cmddb.ExecuteNonQuery());

            return (-1);
        }

        

        public int Ejecutar()
        {
            string sq = sentenciasql;

            foreach (DataRow dr in dsParametros.Tables[0].Rows)
            {
                if (dr["tipo"].ToString() == "NVARCHAR")
                    return(0);


            }

            return (0);

        }

        public void Parameters(string pparametro, object pvalor)
        {
            if (conexion.motor == "SQL")
                cmdsql.Parameters[pparametro].Value = pvalor;
            else
                if (conexion.motor == "OLE")
                    cmdole.Parameters[pparametro].Value = pvalor;
                else
                    if (conexion.motor == "ODBC")
                        cmdodbc.Parameters[pparametro].Value = pvalor;
                    else
                        if (conexion.motor == "PG")
                            cmdpg.Parameters[pparametro].Value = pvalor;


            DataRow[] drp=dsParametros.Tables[0].Select("parametro='"+pparametro+"'");
            if (drp.Length != 0)
                drp[0]["valor"] = pvalor;

            
        }

        public void ParametersAdd(string pparametro, string ptipo, int pancho, string pcampo)
        {
            if (conexion.motor == "SQL")
            {
                if (ptipo.ToUpper()=="NVARCHAR")
                    cmdsql.Parameters.Add(pparametro,SqlDbType.NVarChar, pancho, pcampo);
                else
                    if (ptipo.ToUpper() == "FLOAT")
                        cmdsql.Parameters.Add(pparametro, SqlDbType.Float, pancho, pcampo);
                    else
                        if (ptipo.ToUpper() == "NTEXT")
                            cmdsql.Parameters.Add(pparametro, SqlDbType.NText, pancho, pcampo);
                        else
                        if (ptipo.ToUpper() == "DATETIME")
                            cmdsql.Parameters.Add(pparametro, SqlDbType.DateTime, pancho, pcampo);
                        else
                            if (ptipo.ToUpper() == "INT")
                                cmdsql.Parameters.Add(pparametro, SqlDbType.Int, pancho, pcampo);

            }
            else
                if (conexion.motor == "OLE")
                {
                    if (ptipo.ToUpper() == "NVARCHAR")
                        cmdole.Parameters.Add(pparametro, OleDbType.VarChar, pancho, pcampo);
                    else
                        if (ptipo.ToUpper() == "FLOAT")
                            cmdole.Parameters.Add(pparametro, OleDbType.Double, pancho, pcampo);
                        else
                            if (ptipo.ToUpper() == "NTEXT")
                                cmdole.Parameters.Add(pparametro, OleDbType.VarChar, pancho, pcampo);
                            else
                                if (ptipo.ToUpper() == "DATETIME")
                                    cmdole.Parameters.Add(pparametro, OleDbType.DBTimeStamp, pancho, pcampo);
                                else
                                    if (ptipo.ToUpper() == "INT")
                                        cmdole.Parameters.Add(pparametro, OleDbType.Integer, pancho, pcampo);

                }
                else
                    if (conexion.motor == "ODBC")
                    {
                        if (ptipo.ToUpper() == "NVARCHAR")
                            cmdodbc.Parameters.Add(pparametro, OdbcType.NVarChar, pancho, pcampo);
                        else
                            if (ptipo.ToUpper() == "FLOAT")
                                cmdodbc.Parameters.Add(pparametro, OdbcType.Double, pancho, pcampo);
                            else
                                if (ptipo.ToUpper() == "NTEXT")
                                    cmdodbc.Parameters.Add(pparametro, OdbcType.NText, pancho, pcampo);
                                else
                                    if (ptipo.ToUpper() == "DATETIME")
                                        cmdodbc.Parameters.Add(pparametro, OdbcType.DateTime, pancho, pcampo);
                                    else
                                        if (ptipo.ToUpper() == "INT")
                                            cmdodbc.Parameters.Add(pparametro, OdbcType.Int, pancho, pcampo);
                    }
                    else
                        if (conexion.motor == "PG")
                        {
                            if (ptipo.ToUpper() == "NVARCHAR")
                                cmdpg.Parameters.Add(pparametro, NpgsqlTypes.NpgsqlDbType.Varchar, pancho, pcampo);
                            else
                                if (ptipo.ToUpper() == "FLOAT")
                                    cmdpg.Parameters.Add(pparametro, NpgsqlTypes.NpgsqlDbType.Double, pancho, pcampo);
                                else
                                    if (ptipo.ToUpper() == "NTEXT")
                                        cmdpg.Parameters.Add(pparametro,  NpgsqlTypes.NpgsqlDbType.Varchar, pancho, pcampo);
                                    else
                                        if (ptipo.ToUpper() == "DATETIME")
                                            cmdpg.Parameters.Add(pparametro, NpgsqlTypes.NpgsqlDbType.Date, pancho, pcampo);
                                        else
                                            if (ptipo.ToUpper() == "INT")
                                                cmdpg.Parameters.Add(pparametro, NpgsqlTypes.NpgsqlDbType.Integer, pancho, pcampo);
                        }


            DataRow dr = dsParametros.Tables[0].NewRow();
            dr["parametro"]=pparametro;
            dr["campo"]=pcampo;
            dr["tipo"]=ptipo;
            dr["ancho"]=pancho;
            dsParametros.Tables[0].NewRow();



        }



        public void ParametersAddValor(string pparametro, object pvalor)
        {
            if (conexion.motor == "SQL")
            {
                if (!cmdsql.Parameters.Contains(pparametro))
                    cmdsql.Parameters.AddWithValue(pparametro, pvalor);
                else
                    cmdsql.Parameters[pparametro].Value = pvalor;

            }
            else
                if (conexion.motor == "OLE")
            {
                if(!cmdole.Parameters.Contains(pparametro))
                    cmdole.Parameters.AddWithValue(pparametro, pvalor);
                else
                    cmdole.Parameters[pparametro].Value = pvalor;

            }
            else
                    if (conexion.motor == "ODBC")
            {
                if(!cmdodbc.Parameters.Contains(pparametro))
                    cmdodbc.Parameters.AddWithValue(pparametro, pvalor);
                                else
                    cmdodbc.Parameters[pparametro].Value = pvalor;

            }
            else
                        if (conexion.motor == "PG")
            {
                if(!cmdpg.Parameters.Contains(pparametro))
                    cmdpg.Parameters.AddWithValue (pparametro, pvalor);
                else
                    cmdpg.Parameters[pparametro].Value = pvalor;

            }
            else
                        if (conexion.motor == "MY")
            {
                if(!cmddb.Parameters.Contains(pparametro))
                    cmddb.Parameters.AddWithValue(pparametro, pvalor);
                else
                    cmddb.Parameters[pparametro].Value = pvalor;

            }




        }




        public string CommandText
        {
            
            get {
                if (conexion.motor =="SQL")
                    return(cmdsql.CommandText);
                else
                    if (conexion.motor == "OLE")
                        return (cmdole.CommandText);
                    else
                        if (conexion.motor == "ODBC")
                            return (cmdodbc.CommandText);
                        else
                            if (conexion.motor == "PG")
                                return (cmdpg.CommandText);
                else
                            if (conexion.motor == "MY")
                    return (cmddb.CommandText);


                return (sentenciasql);
                 }
            set {

                sentenciasql = transformsql(value);

                if (conexion.motor == "SQL")
                    cmdsql.CommandText=sentenciasql;
                else
                    if (conexion.motor == "OLE")
                        cmdole.CommandText=sentenciasql;
                    else
                        if (conexion.motor == "ODBC")
                            cmdodbc.CommandText=sentenciasql;
                        else
                            if (conexion.motor == "PG")
                                cmdpg.CommandText = sentenciasql;
                else
                            if (conexion.motor == "MY")
                    cmddb.CommandText = sentenciasql;


            }


        }

        public void estransaccion(Transaction ptrans)
        {

            if (conexion.motor == "SQL")
                cmdsql.Transaction = ptrans.transql;
            else
                if (conexion.motor == "OLE")
                    cmdole.Transaction = ptrans.transole;
                else
                    if (conexion.motor == "ODBC")
                        cmdodbc.Transaction = ptrans.tranodbc;
                    else
                        if (conexion.motor == "PG")
                            cmdpg.Transaction = ptrans.transpg;
            else
                        if (conexion.motor == "MY")
                cmddb.Transaction =(MySql.Data.MySqlClient.MySqlTransaction) ptrans.transdb;



        }

        public void esprocedimiento()
        {
            if (conexion.motor =="SQL")
                cmdsql.CommandType = CommandType.StoredProcedure;
            else
            if (conexion.motor =="PG")
                    cmdpg.CommandType = CommandType.StoredProcedure;
            else
            if (conexion.motor =="MY")
                cmddb.CommandType = CommandType.StoredProcedure;


        }

        public void esnormal()
        {
            if (conexion.motor == "SQL")
                cmdsql.CommandType = CommandType.Text;
            else
            if (conexion.motor == "PG")
                cmdpg.CommandType = CommandType.Text;
            else
            if (conexion.motor == "MY")
                cmddb.CommandType = CommandType.Text;


        }


        public Transaction Transaccion
        {

            get
            {
                //if (conexion.motor == "SQL")
                //      return ();
                //else
                //    if (conexion.motor == "OLE")
                //        return (cmdole.CommandText);
                //    else
                //        if (conexion.motor == "ODBC")
                //            return (cmdodbc.CommandText);
                //return ("");
                return (null);
            }
            set
            {

                if (conexion.motor == "SQL")
                    cmdsql.Transaction  = value.transql;
                else
                    if (conexion.motor == "OLE")
                        cmdole.Transaction = value.transole;
                    else
                        if (conexion.motor == "ODBC")
                            cmdodbc.Transaction = value.tranodbc;
                        else
                            if (conexion.motor == "PG")
                                cmdpg.Transaction = value.transpg;
                else
                            if (conexion.motor == "MY")
                    cmddb.Transaction =(MySql.Data.MySqlClient.MySqlTransaction) value.transdb;


            }


        }

        public int CommandTimeOut
        {
            get
            {
                if (conexion.motor == "SQL")
                    return(cmdsql.CommandTimeout);
                else
                    if (conexion.motor == "OLE")
                       return( cmdole.CommandTimeout);
                    else
                        if (conexion.motor == "ODBC")
                          return(cmdodbc.CommandTimeout);
                        else
                            if (conexion.motor == "PG")
                                return(cmdpg.CommandTimeout);
                else
                            if (conexion.motor == "MY")
                    return (cmddb.CommandTimeout);


                return (0);

            }

            set
            {
                if (conexion.motor == "SQL")
                    cmdsql.CommandTimeout  = value;
                else
                    if (conexion.motor == "OLE")
                        cmdole.CommandTimeout  = value;
                    else
                        if (conexion.motor == "ODBC")
                            cmdodbc.CommandTimeout  = value;
                        else
                            if (conexion.motor == "PG")
                                cmdpg.CommandTimeout  = value;
                else
                            if (conexion.motor == "MY")
                    cmddb.CommandTimeout = value;



            }



        }


    }
}
