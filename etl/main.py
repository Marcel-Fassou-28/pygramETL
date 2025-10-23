import csv
from pygrametl.datasources import CSVSource
from connector import connect_to_database, load_csv_to_mysql, create_fact_table

# --- EXTRACT ---
# Charger les états
with open("data/states.txt", mode="r", encoding="utf-8", newline="") as states_file:
    states_source = CSVSource(states_file, delimiter=';')
    states_dict = {row['StateID']: row['State_Name'] for row in states_source}

# Charger et transformer les clients
transformed_customers = []
with open("data/customers.csv", mode="r", encoding="utf-8", newline="") as customers_file:
    customers_source = CSVSource(customers_file, delimiter=';')
    
    for row in customers_source:
        state_id = row['StateID']
        if state_id and state_id in states_dict:  # éliminer les clients sans état
            row['State_Name'] = states_dict[state_id]
            row['TotalSUM'] = float(row['SUM1']) + float(row['SUM2'])
            row['AverageSUM'] = (float(row['SUM1']) + float(row['SUM2'])) / 2
            transformed_customers.append(row)


# --- LOAD CSV ---
output_file = "output/transformed_customers.csv"
with open(output_file, "w", newline="", encoding="utf-8") as f:
    fieldnames = list(transformed_customers[0].keys())
    writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter=';')
    writer.writeheader()
    for row in transformed_customers:
        writer.writerow(row)

print(f"CSV transformé généré dans : {output_file}")

db_name = "bd_issue"
csv_file = "output/transformed_customers.csv"
    
conn, cur = connect_to_database(db_name)
if conn and cur:
    create_fact_table(cur)
    load_csv_to_mysql(cur, conn, csv_file)
    cur.close()
    conn.close()

