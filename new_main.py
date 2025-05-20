import os
import re
import csv
import sys

basedir = "/home/students/ebusnel/database/"
if not os.path.exists(basedir):
    os.mkdir(basedir)

current_db = os.path.join(basedir, "base")
if not os.path.exists(current_db):
    os.mkdir(current_db)

def create(command):
    pattern = r"^\s*CREATE\s+TABLE\s+(\w+)\s*\(([^)]+)\)\s*$"
    match = re.match(pattern, command, re.IGNORECASE)
    
    if match:
        table_name = match.group(1)
        column_str = match.group(2)
        col_names = [col.strip() for col in column_str.split(",")]
        
        os.makedirs(current_db, exist_ok=True)
        table_path = os.path.join(current_db, f"{table_name}.csv")
        
        if os.path.exists(table_path):
            print(f"Table '{table_name}' already exists.")
            return False

        with open(table_path, "w") as file:
            file.write(",".join(col_names) + "\n")
        
        print(f"Table '{table_name}' created successfully.")
        return True
    else:
        print("Invalid CREATE syntax.")
        return False

def insert(command):
    pattern = r"^\s*INSERT\s+INTO\s+(\w+)\s+VALUES\s*\(([^)]+)\)\s*$"
    match = re.match(pattern, command, re.IGNORECASE)
    
    if match:
        table_name = match.group(1)
        values_str = match.group(2)
        row_values = [val.strip() for val in values_str.split(",")]

        table_path = os.path.join(current_db, f"{table_name}.csv")
        
        if os.path.exists(table_path):
            with open(table_path, "r") as file:
                header = file.readline().strip().split(",")
            
            if len(row_values) != len(header):
                print("Column count does not match table defined.")
                return False

            with open(table_path, "a") as file:
                file.write(",".join(row_values) + "\n")
            print("Row successfully appended.")
            return True
        else:
            print("Table does not exist.")
            return False
    else:
        print("Invalid INSERT syntax.")
        return False

def select_single_table(command):
    match = re.search(r"\bFROM\s+(\w+)", command, re.IGNORECASE)
    if match:
        return match.group(1)
    else:
        print("Invalid FROM syntax.")
        return None

def select_join_tables(command):
    join_match = re.search(r"\bJOIN\s+(\w+)", command, re.IGNORECASE)
    on_match = re.search(r"\bON\s+(\w+\.\w+)\s*=\s*(\w+\.\w+)", command, re.IGNORECASE)

    if join_match and on_match:
        table_name = join_match.group(1)
        left_id = on_match.group(1).strip()
        right_id = on_match.group(2).strip()
        return table_name, left_id, right_id
    else:
        return None, None, None


def select_where(command):
    match = re.search(r"\bWHERE\s+(.+)", command, re.IGNORECASE)
    if match:
        print(match.group(1).strip())
        return match.group(1).strip()
    else:
        return None

def select_columns(command):
    match = re.match(r"^\s*SELECT\s+(.*?)\s+FROM\b", command, re.IGNORECASE)
    if match:
        raw_cols = match.group(1).strip()
        if raw_cols == "*":
            return ["*"]
        return [col.strip() for col in raw_cols.split(",")]
    else:
        print("Invalid SELECT syntax.")
        return ["*"]

def select_display(columns, single_table, join_table, lid, rid, where):
    try:
        table1_path = os.path.join(current_db, f"{single_table}.csv")
        with open(table1_path, "r") as file1:
            reader1 = csv.reader(file1)
            header1 = next(reader1)
            data1 = list(reader1)

        if not join_table:
            final_header = header1
            final_rows = data1
        else:
            table2_path = os.path.join(current_db, f"{join_table}.csv")
            with open(table2_path, "r") as file2:
                reader2 = csv.reader(file2)
                header2 = next(reader2)
                data2 = list(reader2)

            lid_col = lid.split(".")[-1] if lid else None
            rid_col = rid.split(".")[-1] if rid else None

            if lid_col not in header1:
                print(f"Join column '{lid_col}' not found in '{single_table}'.")
                return
            if rid_col not in header2:
                print(f"Join column '{rid_col}' not found in '{join_table}'.")
                return

            lid_idx = header1.index(lid_col)
            rid_idx = header2.index(rid_col)

            header2_renamed = [
                f"{join_table}.{col}" if col in header1 else col
                for i, col in enumerate(header2) if i != rid_idx
            ]
            final_header = header1 + header2_renamed
            final_rows = []

            for row1 in data1:
                val1 = row1[lid_idx].strip().strip("'\"")
                for row2 in data2:
                    val2 = row2[rid_idx].strip().strip("'\"")
                    if val1 == val2:
                        row2_trimmed = row2[:rid_idx] + row2[rid_idx + 1:]
                        joined_row = row1 + row2_trimmed
                        final_rows.append(joined_row)

        if where:
            match = re.match(r"^\s*([^=]+)\s*=\s*['\"]?([^'\"]+)['\"]?$", where.strip())
            if match:
                where_col = match.group(1).strip().split(".")[-1]
                where_val = match.group(2).strip()
                if where_col in final_header:
                    where_idx = final_header.index(where_col)
                    final_rows = [
                        row for row in final_rows
                        if row[where_idx].strip().strip("'\"") == where_val
                    ]
                else:
                    #print(f"Column '{where_col}' not found in result.")
                    return
            else:
                #print("Invalid WHERE syntax.")
                return

        if columns == ["*"]:
            selected_indices = list(range(len(final_header)))
            selected_header = final_header
        else:
            selected_indices = []
            selected_header = []
            for col in columns:
                base_col = col.split(".")[-1]
                if col in final_header:
                    idx = final_header.index(col)
                elif base_col in final_header:
                    idx = final_header.index(base_col)
                else:
                    #print(f"Column '{col}' not found.")
                    return
                selected_indices.append(idx)
                selected_header.append(final_header[idx])

        print("\t".join(selected_header))
        for row in final_rows:
            print("\t".join(str(row[i]) for i in selected_indices))

    except FileNotFoundError as e:
       #print(f"File error: {e}")
        return


def select(command):
    single_table = select_single_table(command)
    join_table, lid, rid = select_join_tables(command)
    where_clause = select_where(command)
    columns = select_columns(command)

    select_display(columns, single_table, join_table, lid, rid, where_clause)

def show_tables():
    if not os.path.exists(current_db):
        print("No active database or database does not exist.")
        return
    
    tables = [f[:-4] for f in os.listdir(current_db) if f.endswith(".csv")]
    print(f"Tables in 'current database:")
    for t in tables:
        print(f"- {t}")

def describe_table(tablename):
    table_path = os.path.join(current_db, f"{tablename}.csv")
    if os.path.exists(table_path):
       with open(table_path, "r") as file:
            header = next(csv.reader(file))
            print(f"Columns in '{tablename}':")
            for col in header:
                print(f"- {col}")
    else:
        print(f"Table '{tablename}' does not exist in the current database.")

def main():
    command = sys.argv[1]
    command_upper = command.upper()

    if command_upper.startswith("CREATE"):
        create(command)

    elif command_upper.startswith("INSERT"):
        insert(command)

    elif command_upper.startswith("SELECT"):
        select(command)

    elif command_upper.startswith("SHOW DATABASES"):
        show_databases()

    elif command_upper.startswith("DESC"):
        tablename = command.split(" ",1)[1].strip()
        describe_table(tablename)
    
if __name__ == "__main__":
    main()

