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
    //capa de datos para independizar el tipo de base de datos de la aplicación
    
    public class Connection
    {
        public string motor;//SQL,OLE,ODBC,PG
        public string cadenaconexion;
        public SqlConnection conexionsql;
        public OleDbConnection conexionole;
        public OdbcConnection conexionodbc;
        public Npgsql.NpgsqlConnection conexionpg;
        public IDbConnection conexiondb;

        public Connection()
        {

        }

        public Connection(string pconexion,string pmotor)
        {
            motor = pmotor;
            cadenaconexion = pconexion;
            if (motor == "SQL")
                conexionsql = new SqlConnection(cadenaconexion);
            else
                if (motor == "OLE")
                    conexionole = new OleDbConnection(cadenaconexion);
                else
                    if (motor == "ODBC")
                        conexionodbc = new OdbcConnection(cadenaconexion);
                    else
                        if (motor == "PG")
                            conexionpg = new  Npgsql.NpgsqlConnection(cadenaconexion);
            else
                        if (motor == "MY")
                conexiondb = (IDbConnection) new  MySql.Data.MySqlClient.MySqlConnection(cadenaconexion);

        }

        public void Open()
        {
            if (motor == "SQL")
                conexionsql.Open();
            else
                if (motor == "OLE")
                    conexionole.Open();
                else
                    if (motor == "ODBC")
                        conexionodbc.Open();
                    else
                        if (motor == "PG")
                            conexionpg.Open();
            else
                        if (motor == "MY")
                conexiondb.Open();

        }

        public void Close()
        {
            if (motor == "SQL")
                conexionsql.Close();
            else
                if (motor == "OLE")
                    conexionole.Close();
                else
                    if (motor == "ODBC")
                        conexionodbc.Close();
                    else
                        if (motor == "PG")
                            conexionpg.Close();
            else
                        if (motor == "MY")
                conexiondb.Close();

        }

        //abrir conexiones
        public void abrir(string pconexion, ref SqlConnection  pcone)
        {
           pcone = new SqlConnection(pconexion);
        }

        public void abrir(string pconexion,ref OleDbConnection pcone)
        {
            pcone= new OleDbConnection(pconexion);
        }

        public void abrir(string pconexion,ref OdbcConnection pcone)
        {
            pcone=new OdbcConnection(pconexion);
        }

        public void abrir(string pconexion, ref  Npgsql.NpgsqlConnection pcone)
        {
            pcone = new  Npgsql.NpgsqlConnection(pconexion);
        }

        public void abrir(string pconexion, ref IDbConnection  pcone)
        {
            pcone =(IDbConnection) new MySql.Data.MySqlClient.MySqlConnection(pconexion);
        }

        public void abrirconexion(string pconexion)
        {
            cadenaconexion = pconexion;
            if (motor == "SQL")
                abrir(pconexion, ref conexionsql);
            else
                if (motor == "OLE")
                    abrir(pconexion, ref conexionole);
                else
            if (motor == "ODBC")
                abrir(pconexion, ref conexionodbc);
            else
                if (motor == "PG")
                    abrir(pconexion, ref conexionpg);
            else
                if (motor == "MY")
                abrir(pconexion, ref conexiondb);


        }



        public Transaction BeginTransaction()
        {
            Transaction trans;
            trans=new Transaction();
            trans.motor =this.motor;
            if (motor == "SQL")
                trans.transql = conexionsql.BeginTransaction();
            else
            if (motor == "OLE")
                trans.transole = conexionole.BeginTransaction();
            else
            if (motor == "ODBC")
                trans.tranodbc = conexionodbc.BeginTransaction();
            else
                if (motor == "PG")
                    trans.transpg = conexionpg.BeginTransaction();
            else
                if (motor == "MY")
                trans.transdb = conexiondb.BeginTransaction();


            return (trans);
        }

        public Transaction BeginTransaction(IsolationLevel pnivel)
        {
            Transaction trans;
            trans = new Transaction();
            trans.motor = this.motor;
            if (motor == "SQL")
                trans.transql = conexionsql.BeginTransaction(pnivel);
            else
                if (motor == "OLE")
                    trans.transole = conexionole.BeginTransaction(pnivel);
                else
                    if (motor == "ODBC")
                        trans.tranodbc = conexionodbc.BeginTransaction(pnivel);
                    else
                        if (motor == "PG")
                            trans.transpg = conexionpg.BeginTransaction(pnivel);
            else
                        if (motor == "MY")
                trans.transdb = conexiondb.BeginTransaction(pnivel);


            return (trans);
        }

        public string ConnectionString
        {
            get
            {
                return (cadenaconexion);
            }

            set
            {
                cadenaconexion = value;

            }

        }

        public System.Data.ConnectionState State
        {
            get
            {
                if (motor == "SQL")
                    return (conexionsql.State);
                else
                    if (motor == "OLE")
                        return (conexionole.State);
                    else
                        if (motor == "ODBC")
                            return (conexionodbc.State);
                        else
                            if (motor == "PG")
                                return (conexionpg.State);
                else
                            if (motor == "MY")
                    return (conexiondb.State);

                else
                    return (conexionsql.State);

            }
            set
            {

            }

        }



    }   
}
