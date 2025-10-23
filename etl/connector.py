import mysql.connector
from mysql.connector import Error
import csv

def connect_to_database(db_name):
    """Connexion à MySQL et retourne la connexion et le curseur"""
    try:
        connection = mysql.connector.connect(
            host="127.0.0.1",
            port=3306,
            user="root",
            password="",
            database=db_name
        )
        if connection.is_connected():
            cursor = connection.cursor()
            print(f"Connexion réussie à la base {db_name}")
            return connection, cursor
    except Error as e:
        print(f"Erreur de connexion : {e}")
        return None, None

def create_fact_table(cursor):
    """Crée la table fact_custome si elle n'existe pas"""
    try:
        create_query = """
        CREATE TABLE IF NOT EXISTS fact_custome (
            Customer_Id INT PRIMARY KEY,
            CustomerName VARCHAR(100),
            Customer_adresse VARCHAR(255),
            StateID INT,
            State_Name VARCHAR(50),
            id2 INT,
            tempsInsc DATE,
            SUM1 FLOAT,
            SUM2 FLOAT,
            TotalSUM FLOAT,
            AverageSUM FLOAT
        );
        """
        cursor.execute(create_query)
        print("Table fact_table créée ou existante.")
    except Error as e:
        print(f"Erreur lors de la création de la table : {e}")

def load_csv_to_mysql(cursor, connection, csv_file_path):
    """Charge un fichier CSV dans la table fact_table"""
    try:
        with open(csv_file_path, mode='r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f, delimiter=';')
            for row in reader:
                try:
                    insert_query = """
                    INSERT INTO fact_custome (
                        Customer_Id, CustomerName, Customer_adresse, StateID, State_Name,
                        id2, tempsInsc, SUM1, SUM2, TotalSUM, AverageSUM
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """
                    values = (
                        int(row['Customer_Id']),
                        row['CustomerName'],
                        row['Customer_adresse'],
                        int(row['StateID']),
                        row['State_Name'],
                        int(row['id2']),
                        row['tempsInsc'],
                        float(row['SUM1']),
                        float(row['SUM2']),
                        float(row['TotalSUM']),
                        float(row['AverageSUM'])
                    )
                    cursor.execute(insert_query, values)
                except ValueError as ve:
                    print(f"Conversion impossible pour la ligne {row}: {ve}")
                except Error as e:
                    print(f"Erreur d'insertion pour la ligne {row}: {e}")
        connection.commit()
        print(f"Données chargées depuis {csv_file_path} dans fact_table.")
    except FileNotFoundError:
        print(f"Fichier CSV introuvable : {csv_file_path}")
    except Error as e:
        print(f"Erreur lors du chargement CSV : {e}")


if __name__ == "__main__":
    db_name = "bd_issue"
    csv_file = "output/transformed_customers.csv"
    
    conn, cur = connect_to_database(db_name)
    if conn and cur:
        create_fact_table(cur)
        load_csv_to_mysql(cur, conn, csv_file)
        cur.close()
        conn.close()


