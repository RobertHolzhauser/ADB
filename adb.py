# Robert Holzhauser - Final Project - Fall 2025 CSCIE7 Harvard Extension School
# purpose: to explore creating a relational database type program in python

import os       # https://www.w3schools.com/python/ref_module_os.asp & https://docs.python.org/3/library/os.html#module-os
import sys       # for sys.exit() for catch & count of argvs # https://www.w3schools.com/python/ref_module_sys.asp & https://docs.python.org/3/library/sys.html#module-sys
import csv       # https://www.w3schools.com/python/ref_module_csv.asp & https://docs.python.org/3/library/csv.html#module-csv
import argparse  # https://www.w3schools.com/python/ref_module_argparse.asp & https://docs.python.org/3/library/argparse.html & https://docs.python.org/3/howto/argparse.html#argparse-tutorial
from datetime import datetime  # https://www.w3schools.com/python/ref_module_datetime.asp
from operator import itemgetter # https://docs.python.org/3/howto/sorting.html#sortinghowto

# v 0.0 = initial version assume all files are csv
verbose = False         # config setting from command line

# parse SQL command into component parts
def sql_parse():
    parser = argparse.ArgumentParser(
                    prog='ADB v0.0.0',
                    description='Python based simple database system.',
                    epilog='Supports some simple SQL-like command line arguments')

    group = parser.add_mutually_exclusive_group()
    group.add_argument("-ct","--create_table", type=str, help="create a table")    # TableName -- column names are entered via keyboard interaction
    group.add_argument("-s","--select", type=str, help="display the data in the table")
    parser.add_argument("-f","--from_table", type=str, help="list tables to use in the query")
    group.add_argument("-st","--show_tables", type=str, help="list avaible tables :: usage = --show_tables y")
    parser.add_argument("-w","--where", type=str, help="specify criteria to limit the rows retrieved") # non empty where clause triggers manual keyboard entry routine for criteria.  Limit of one criteria per field
    parser.add_argument("-ob","--order_by", type=str, help="specify the sort column")  #  this is the column number - zero based, aka ordinal position
    group.add_argument("-i", "--insert", type=str,  help="insert rows into the specified table") # this parameter is the table name
    group.add_argument("-u","--update", type=str, help="change values in set columns") # this parameter is the table name
    parser.add_argument("-set","--set", type=str, help="specify column to change") # this parameter is the column to alter the value of
    parser.add_argument("-nv","--new_value", type=str, help="specify the new value for the column") # this parameter is the new value for the column
    group.add_argument("-d","--delete", type=str, help="delete specified rows from a table") # this parameter is the table name
    group.add_argument("-tr","--truncate", type=str,  help="empty all rows from a table, but keep the table structure intact") # this parameter is the table name
    group.add_argument("-at","--alter_table", type=str, help="alter a table by changing columns") # this parameter is the table name
    parser.add_argument("-ac","--alter_col", type=str, help="the column to alter") # this parameter is the column name to alter
    parser.add_argument("-rn","--new_col_name", type=str, help="the new column name to change to") # this parameter is the column name to alter
    group.add_argument("-rt","--rename_table", type=str, help="the table name to change")
    parser.add_argument("-ntn","--new_table_name",type=str, help="the new table name")
    group.add_argument("-dt","--drop_table", type=str, help="drop a table - aka delete the entire table, including the table structure")  # this parameter is the table name to drop
    parser.add_argument("-v","--verbose", type=int, choices=[0, 1], default=0, help="verbose mode = how much of process tracing comments do you want to see: 0 = off / none, 1 = full verbose")

    args = parser.parse_args()  # extract the arguments

    if len(sys.argv) < 2:
        sys.exit("Please enter sufficient parameters. Use -h for help")

    if args.verbose:
        verbose = True
        print("verbose turned on")

    if args.show_tables:
        show_tables()

    if args.select:
        from_table = ''
        where = ''
        order_by = ''
        if args.verbose == 1:
            verbose = True
        else:
            verbose = False

        if args.from_table:
            from_table = args.from_table
        else:
            sys.exit("from table is required for select action")

        if args.where:
            where = args.where

        if args.order_by:
            order_by = args.order_by
        sql_select(args.select, from_table, where, order_by, verbose)

    if args.update:
        if args.where:
            where = args.where
        else:
            where = ""

        if args.verbose == 1:
            verbose = True
        else:
            verbose = False

        if args.set:
            setcols = args.set
        else:
            sys.exit("A single set column must be defined for an update")

        if args.new_value:
            new_val = args.new_value
        else:
            sys.exit("The new value for the column must be specified for an update statement")

        sql_update(args.update, where, setcols, new_val, verbose)

    if args.insert:
        if args.verbose == 1:
            verbose = True
        else:
            verbose = False
        sql_insert(args.insert, verbose)

    if args.delete:
        if args.where:
            where = args.where
        else:
            where = ""

        if args.verbose == 1:
            verbose = True
        else:
            verbose = False
        sql_delete(args.delete, where, verbose)

    if args.truncate:
        if args.verbose == 1:
            verbose = True
        else:
            verbose = False
        sql_truncate(args.truncate, verbose)

    if args.create_table:
        if args.verbose == 1:
            verbose = True
        else:
            verbose = False
        sql_create_table(args.create_table, verbose)

    if args.alter_table:
        if args.alter_col:
            alter_col = args.alter_col
        if args.new_col_name:
            new_col_name = args.new_col_name
        if args.verbose == 1:
            verbose = True
        else:
            verbose = False
        sql_alter_table(args.alter_table, alter_col, new_col_name, verbose)

    if args.rename_table:
        if args.verbose == 1:
            verbose = True
        else:
            verbose = False

        if args.new_table_name:
            sql_alter_table_name(args.rename_table, args.new_table_name, verbose)
        else:
            sys.exit("new_table_name parameter required for rename table")

    if args.drop_table:
        if args.verbose == 1:
            verbose = True
        else:
            verbose = False
        sql_drop_table(args.drop_table, verbose)

# print input sql commands, and more if flag is set
def sql_verbose(verbose,sql_str):
    if verbose == True:
        print(sql_str)

def get_table(file_name):
    try:
        my_data = []
        # read to file into memory
        with open(file_name) as file_csv:
            my_reader = csv.reader(file_csv)
            for row in my_reader:
                my_data.append(row)
        return my_data
    except Exception as e:                                                   # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit(f"Error getting data from file {file_name} :: {e}")


# print-out contents of a table
def sql_select(select_str, from_str, where, order_by, verbose):
    sql_verbose(verbose,select_str + "::" + from_str + "::" + where + "::" + order_by)
    try:
        from_str = from_str.strip().replace(".csv","")
        file_name = from_str + ".csv"                   # strip whitespace and double check to avoid multiple ".csv" in file_name, and appending ".csv" to file_name
        if file_name not in os.listdir():
            sys.exit("table not found")
        else:
            my_data = get_table(file_name)
            my_display = []
            cnt = 0
            if len(where) > 0:
                header = get_csv_first_row(file_name)

                # get criteria for where clause
                col_criteria = keyboard_entry_where(header, from_str)
                # add header indexes for col_criteria to col_criteria dictionary
                for col in col_criteria.keys():
                    if col in header:
                        col_criteria[col].append(header.index(col))  # move from value = [operator, value] to value = [operator, value, header_index]
                    else:
                        # print("col NOT in header")
                        col_criteria[col].append(-1)

                # go through in-memory table and conditionally change values
                j = 0 # point to table col

                criteria_count = len(col_criteria)

                rng = len(my_data)
                cnt = rng
                for i in range(1, rng):
                    eval_count = 0  # when the eval count matchs the number of criteria (aka len of col_criteria), THEN update
                    for valset in list([item for item in list(col_criteria.values()) if item != None and item[2] >= 0]):  # these are criteria fields
                        if len(my_data[i]) > 2:
                            # greater than
                            if valset[0] == "g" and my_data[i][valset[2]] > valset[1]:
                                eval_count += 1
                            elif valset[0] == "l" and my_data[i][valset[2]] < valset[1]:
                                eval_count += 1
                            elif valset[0] == "e" and my_data[i][valset[2]] == valset[1]:
                                eval_count += 1
                            j += 1     # keep synced with table col

                    if eval_count == criteria_count and eval_count > 0:
                        my_display.append(my_data[i])  # remove row

                cnt = len(my_display)
            else:  # no where clause
                cnt = len(my_data)
                my_display = my_data.copy()

            # display results
            print(from_str)                 # print table
            for i in range(len(from_str)):  # under line table name
                print('-', end="")
            print()   # new line

            # sort results by specified order column # - if order by is specified
            if len(order_by.strip()) > 0:
                sorted_data = sorted(my_display, key=itemgetter(int(order_by)))      #  https://docs.python.org/3/howto/sorting.html#sortinghowto
                for row in sorted_data:
                    print(row)
            else:
                for row in my_display:
                    print(row)

            print(f"returned {cnt} rows")
    except Exception as e:                          # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit(f"Error selecting data from table {from_str} :: to be ordered by {order_by} :: {e}")

# print a list of all tables in the database
def show_tables():
    print('TABLES')
    print('------')
    tables = os.listdir()
    cnt = 0
    for table in tables:
        if table.endswith(".csv"):  # assumes all .csv files are tables
            print(table.replace(".csv",""))
            cnt += 1
    print(f"displaying {cnt} tables")
    sys.exit() # for case where called from other function

# this assumes existence of both files has been checked.  from_file will be deleted to keep paradigm that all .csv files are tables
def insert_data_from_file(from_file,to_table):
    try:
        to_file = to_table.strip().replace(" ","").replace(".csv","") + ".csv"     # double check we aren't stacking ".csv" at the end of the table name - before appending another .csv
        from_data = []
        to_data = []
        cnt = 0

        # read from file into memory
        with open(from_file) as from_file_csv:
            from_reader = csv.reader(from_file_csv)
            row_data = list(from_reader)
            rng = len(row_data)
            for i in range(1, rng):             # skip header row in from file
                from_data.append(row_data[i])
                cnt += 1

        # read to file into memory
        with open(to_file) as to_file_csv:
            to_reader = csv.reader(to_file_csv)
            to_data = list(to_reader)
            for to_row in to_reader:
                to_data.append(to_row)

        # add new rows to in memory collection of existing rows
        for row in from_data:       # in memory add
            to_data.append(row)

        # rename to_file
        temp_old_to_file = to_table + "_" + str(datetime.now()) + ".csv"
        os.rename(to_file, temp_old_to_file)

        with open(to_file,'w') as new_to_file_csv:
            to_writer = csv.writer(new_to_file_csv)
            to_writer.writerows(to_data)

        # delete temp file
        os.remove(temp_old_to_file)
        print(f"{cnt} rows inserted")
    except Exception as e:                                          # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit(f"Error inserting data into table {to_table} from file {from_file}")
    return True

# check if a table exists (as a .csv file in curr directory)
def table_exists(table_name):
    if not table_name.endswith(".csv"):
        table_name = table_name + ".csv"
    if os.path.exists(table_name):
        return True
    return False

# this captures the header row
def get_csv_first_row(file_name):
    with open(file_name, newline='') as csvfile:
        reader = csv.reader(csvfile)
        first_row = next(reader)
        return first_row

def insert_keyboard_data(new_data_list,to_file):
    try:
        to_data = []

         # read to file into memory in to_data list
        with open(to_file, newline='') as to_file_csv:
            to_reader = csv.reader(to_file_csv)
            for to_row in to_reader:
                to_data.append(to_row)
        for row in new_data_list:       # load in memory
            to_data.append(row)

         # rename to_file
        temp_old_to_file = to_file.replace(".csv","") + "_" + str(datetime.now()) + ".csv"
        os.rename(to_file, temp_old_to_file)

        with open(to_file, 'w') as new_to_file_csv:
            to_writer = csv.writer(new_to_file_csv)
            to_writer.writerows(to_data)

        # delete temp file
        os.remove(temp_old_to_file)
        print(f"row inserted into {to_file.replace(".csv","")}.")
    except Exception as e:                                          # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit(f"Error inserting data into table {to_file.replace(".csv","")} from keyboard entry:: ")
    return True

# - file magic -- read existing file into a list and return it
def keyboard_insert(file_name):
    try:
        #step 1 - get cols list
        col_list = get_csv_first_row(file_name)
        new_data = []
        row_list = []
         #step 2 - get values for each column from keyboard entry
        for col in col_list:
            val = input(f"Enter value for {col} column: ")
            row_list.append(val)
        new_data.append(row_list)
        insert_keyboard_data(new_data,file_name)
        new_data = []
        act = input("Would you like to insert another row? Y/N: ")
        while act.lower() == "y":
            row_list = []
            for col in col_list:
                val = input(f"Enter value for {col} column: ")
                row_list.append(val)
            new_data.append(row_list)
            insert_keyboard_data(new_data, file_name)
            new_data = []
            act = input("Would you like to insert another row? Y/N: ")

    except Exception as e:                                                  # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit(f"ERROR inserting into {file_name.replace('.csv','')} {e}")


def sql_insert(insert_str, verbose):
    sql_verbose(verbose,insert_str)
    while not table_exists(insert_str):
        option = input("It looks like that table name doesn't exist. Either re-enter (r) the name, or create (ct) a new table, or show tables (st): ")
        while option.lower() not in ('r','re-enter','reenter','enter','ct', 'c', 'create table', 'create', 'show', 'show tables', 'st'):
            option = input(f"Since {insert_str} doesn't seem to exist, either re-enter (r), create a new table (ct) or exit (x): ")
            if option.lower() in ('x', 'exit'):
                sys.exit('Exit option chosen')

        if option in ('r','re-enter','reenter','enter'):
            insert_str = input("Enter the table name: ")
        elif option in ('ct', 'c', 'create table', 'create'):
            insert_str = sql_create_table(insert_str)
        else:
            show_tables()
        # --- here we now have a good table name and move to the actual insert

    inp_method = input(f"Would you like to insert data into {insert_str} with a file (f), or keyboard entry (k)? ")
    while inp_method.lower() not in ('f', 'file', 'keyboard', 'k', ):
        inp_method = input("Please enter either: file (f), keyboard (k), or exit (x): ")
        if inp_method.lower() in ('x','exit'):
            sys.exit("Exit option chosen")

    if inp_method in ('f','file'):
        file_name = input(f"What is the name of the file to insert into the {insert_str}: ")
        if table_exists(file_name):
            insert_data_from_file(file_name,insert_str)
        else:
            sys.exit(f"Unable to locate input file - {file_name}")
    elif inp_method in ('k', 'keyboard'):
        keyboard_insert(insert_str + ".csv")
    else:
        sys.exit("Unknown sql_insert action")

# manual keyboard entry for where clause - one criteria per column to make simple dictionary implementation of criteria
# returns a dictionary of criteria
def keyboard_entry_where(header, table_str):
    try:
        # validate header
        if  len(header) == 0:
            raise Exception("Empty Header row")     # https://www.w3schools.com/python/python_try_except.asp
        else:
            col_criteria = {}       # key = col, value = [operator, value]
            for col in header:
                if "y" == input(f"Add criteria for column {col}? Y/N ").lower():
                    operator = input(f"Is the criteria for {col} Greater Than (G), Less Than (L) or Equal (E), or exit (x)? ").lower()
                    while operator not in (["g","l","e","x"]):
                        operator = input(f"Is the criteria for {col} Greater Than (G), Less Than (L) or Equal (E), or exit (x)? ").lower()
                    if operator == "x":
                        continue
                    else:
                        # make readable operator text
                        operator_txt = ""
                        if operator == "e":
                            operator_txt = "Equal To"
                        elif operator == "g":
                            operator_txt = "Greater Than"
                        else: operator_txt = "Less Than"

                        val = input(f"What is the value in {col} to check if it's {operator_txt}?  ").lower()
                        while len(val) == 0:
                            val = input(f"What is the value in {col} to check if it's {operator_txt}?  ").lower()
                        col_criteria[col] = [operator, val]

            return col_criteria
    except Exception as e:                                  # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit(f"ERROR generating where clause for table {table_str} :: {e}")

# change specified row and col values -- only one col can be set at a time in initial version
def sql_update(update_str, where_str, set_cols, new_val, verbose):
    sql_verbose(verbose, update_str + "::" + where_str + "::" + set_cols + "::" + new_val)
    try:
        file_name = update_str.strip().replace(".csv","").replace(" ","") + ".csv"          # double check to avoid multiple ".csv" in file_name, and appending ".csv" to file_name
        header = get_csv_first_row(file_name)
        cnt = 0
        # get col index to update
        indx = header.index(set_cols)
        tmp_tble = []   # in memory copy of table data

         # first read the file into a list
        with open(file_name, newline='') as upd_csv_readfile:
            update_reader = csv.reader(upd_csv_readfile)
            for row in update_reader:
                tmp_tble.append(row)

         # update all option - if where clause is empty
        if len(where_str.strip()) == 0:

            # update in memory
            for i in range(1, len(tmp_tble)):  # skip row 0 as this is header, which we are NOT changing here
                tmp_tble[i][indx] = new_val

             # write the changed list back to the file - overwriting it's previous contents
            with open(file_name, 'w', newline='') as upd_csv_file:
                update_writer = csv.writer(upd_csv_file)
                for i in range(len(tmp_tble)):
                    update_writer.writerow(tmp_tble[i])
                    cnt += 1

            print(f"{cnt} rows updated")
        # where clause defined
        else:
            col_criteria = keyboard_entry_where(header, update_str)

            # add header indexes for col_criteria to col_criteria dictionary
            for col in col_criteria.keys():
                if col in header:
                    col_criteria[col].append(header.index(col))  # move from value = [operator, value] to value = [operator, value, header_index]
                else:
                    col_criteria[col].append(-1)              #

            # go through in-memory table and conditionally change values
            j = 0 # point to table col
            criteria_count = len(col_criteria)
            rng = len(tmp_tble)
            for i in range(1, rng):
                eval_count = 0  # when the eval count matchs the number of criteria (aka len of col_criteria), THEN update
                for valset in list([item for item in list(col_criteria.values()) if item[2] >= 0]):  # these are criteria fields
                    if len(tmp_tble[i]) > 2:        # prevent list out of range
                        # greater than
                        if valset[0] == "g" and tmp_tble[i][valset[2]] > valset[1]:
                            eval_count += 1
                        elif valset[0] == "l" and tmp_tble[i][valset[2]] < valset[1]:
                            eval_count += 1
                        elif valset[0] == "e" and tmp_tble[i][valset[2]] == valset[1]:
                            eval_count += 1

                    j += 1     # keep synced with table col

                if eval_count == criteria_count:    # matches criteria, so update
                    tmp_tble[i][indx] = new_val
                    cnt += 1

             # write the changed list back to the file - overwriting it's previous contents
            with open(file_name, 'w', newline='') as upd_csv_file:
                update_writer = csv.writer(upd_csv_file)
                for i in range(len(tmp_tble)):
                    update_writer.writerow(tmp_tble[i])

            print(f"{cnt} rows updated")
    except Exception as e:                                      # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit(f"ERROR updating table {update_str} :: {e}")


# remove specified rows only when a where clause is provided.  Otherwise, delete all except for header row
def sql_delete(delete_str, where_str, verbose):
    sql_verbose(verbose, delete_str + "::" + where_str)
    try:
        file_name = delete_str.strip().replace(" ","").replace(".csv","") + ".csv"  # remove any existing .csv to avoid stacking the name, and append ".csv
        header = get_csv_first_row(file_name)
        cnt = 0

        # delete all option - if where clause is empty
        if len(where_str.strip()) == 0:
            with open(file_name, 'r', newline='') as cnt_csv_file:
                cnt_reader = csv.reader(cnt_csv_file)
                cnt = len(list(cnt_reader)) - 1

            with open(file_name, 'w', newline='') as del_csv_file:
                del_writer = csv.writer(del_csv_file)
                del_writer.writerow(header)                     # overwrite the file with the header row

        # where clause defined
        else:
            tmp_tble = get_table(file_name)
            col_criteria = keyboard_entry_where(header, delete_str)

            # add header indexes for col_criteria to col_criteria dictionary
            for col in col_criteria.keys():
                if col in header:
                    col_criteria[col].append(header.index(col))  # move from value = [operator, value] to value = [operator, value, header_index]
                else:
                    col_criteria[col].append(-1)              #

            # go through in-memory table and conditionally change values
            j = 0 # point to table col
            criteria_count = len(col_criteria)
            rng = len(tmp_tble)

            # make a dictionary copy that has the key as the index to avoid offset index issue when trying to pop values and tracking a shrinking table
            tmp_dict = {}
            for i in range(1, rng):
                tmp_dict[i] = tmp_tble[i]


            for i in range(1, rng):
                eval_count = 0  # when the eval count matchs the number of criteria (aka len of col_criteria), THEN update
                for valset in list([item for item in list(col_criteria.values()) if item[2] >= 0]):  # these are criteria fields
                    if len(tmp_tble[i]) > 2:
                        # greater than
                        if valset[0] == "g" and tmp_tble[i][valset[2]] > valset[1]:
                            eval_count += 1      # track matched criteria
                        elif valset[0] == "l" and tmp_tble[i][valset[2]] < valset[1]:
                            eval_count += 1      # track matched criteria
                        elif valset[0] == "e" and tmp_tble[i][valset[2]] == valset[1]:
                            eval_count += 1      # track matched criteria

                        j += 1     # keep synced with table col
                if eval_count == criteria_count and eval_count > 0:
                    tmp_dict[i] = []  # remove row
                    cnt += 1

            tmp_file = file_name.replace(".csv","") + str(datetime.now()) + ".csv"
            os.rename(file_name, tmp_file)

            # over-write file
            with open(file_name, 'w', newline='') as new_file_copy:
                del_writer2 = csv.writer(new_file_copy)
                del_writer2.writerow(header)
                for i in range(1, rng):
                    if len(tmp_dict[i]) > 0:
                        del_writer2.writerow(tmp_dict[i])

            os.remove(tmp_file)
            print(f"{cnt} rows deleted")
    except Exception as e:                                  # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit(f"ERROR deleting from table {delete_str} :: {e}")


# empty all rows from the table (preserve the header)
def sql_truncate(truncate_str, verbose):
    sql_verbose(verbose, truncate_str)
    try:
        file_name = truncate_str.strip().replace(" ","").replace(".csv","") + ".csv"
        col_list = []
        with open(file_name, newline='') as csvfile:
            reader = csv.reader(csvfile)
            col_list = next(reader)

        temp_old_file_name = truncate_str + "_" + str(datetime.now()) + ".csv"   # rename original
        os.rename(file_name, temp_old_file_name)

        with open(file_name,'w', newline='') as newcsvfile:     # write header row to new verion of file
            new_writer = csv.writer(newcsvfile)
            new_writer.writerow(col_list)

        os.remove(temp_old_file_name)                   # with write to new file successful, can remove backup copy of old file
        print(f"Truncated table {truncate_str}")
    except Exception as e:                                  # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit(f"ERROR on truncate {truncate_str}")


# parse table name and columns
def create_table_parser(create_table_str):
    # Get table name
    try:
        table_str = create_table_str.strip().replace(".csv","").replace(" ","")    # remove white space from begining and end  https://www.w3schools.com/python/ref_string_strip.asp
    except Exception as e:                          # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit("Create Table Snytax Issue")
    return table_str


def create_cols_input(table_name):
    try:
        cols_list = []
        col_name = input(f"Please enter the first column name for the {table_name} table: ")
        cols_list.append(col_name)

        while len(col_name) > 1:
            col_name = input(f"Please enter the next column name for the {table_name} table: ")
            if len(col_name) > 0:
                cols_list.append(col_name)
            else:
                return cols_list
    except Exception as e:                  # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit("Exception:  Create Columns Issue")


# syntax for create table --> TableName  .. columns entered through keyboard interaction
def sql_create_table(create_table_str, verbose):
    sql_verbose(verbose,create_table_str)
    try:
        table_name = create_table_parser(create_table_str)
        cols_list = create_cols_input(table_name)           # --> have user input list of cols
        number_cols = str(len(cols_list))
        sql_verbose(verbose,"table_name = " + table_name + " , cols_list = " + number_cols)
        file_name = table_name + '.csv'
        with open(file_name, 'w', newline='') as csvfile:
            fieldnames = cols_list
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
        print(f"Table {table_name} created successfully.")
    except Exception as e:                                          # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit("sql_create_table failed")

# rename a column
def sql_alter_table(alter_table_str, alter_col, new_col_name, verbose):
    sql_verbose(verbose, alter_table_str + "::" + alter_col + "::" + new_col_name)
    try:
        if not alter_table_str.endswith(".csv"):
            file_name = alter_table_str + ".csv"
        else:
            file_name = alter_table_str
        temp_row = []
        with open(file_name, newline ='') as csvfile:     #  https://docs.python.org/3/library/csv.html#module-csv
            reader = csv.reader(csvfile)
            row = reader[0]             # row returned as list of strings -- this is the header row, which has column names
            for col in row:
                if col == alter_col:
                    col = new_col_name      # change col name
                temp_row.append(col)        # copy header row

            temp_file = alter_table_str + str(datetime.now()) + ".csv"  # write data from old file to new file
            with open(temp_file, 'w', newline = '') as newversion:            #
                writer = newversion.writer(temp_file)
                writer.writerow(temp_row)                               # paste the new header row
                for i in range(1, len(list(reader))):                   # copy the rest of the data
                    writer.writerow(reader[i])

        os.remove(alter_table_str)                  # delete old version of file
        os.rename(temp_file, alter_table_str)       # name the temp file as the original file
        print(f"Table {alter_table_str} Column {alter_col} changed to {new_col_name}")
    except Exception as e:                          # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit("ERROR - sql_alter_table")


# rename table
def sql_alter_table_name(alter_table_str, new_name, verbose):
    sql_verbose(verbose, alter_table_str + "::" + new_name)
    try:
        if not alter_table_str.endswith(".csv"):
            file_name = alter_table_str + ".csv"
        else:
            file_name = alter_table_str

        if not new_name.endswith(".csv"):
            new_file_name = new_name + ".csv"
        else:
            new_file_name = new_name

        os.rename(file_name, new_file_name)
        print(f"{alter_table_str} Table renamed successfully to {new_name}.")
    except Exception as e:                      # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit(f"Table rename Error on rename of {alter_table_str} to {new_name}:: Error {e}")


# delete a table
def sql_drop_table(drop_table_str, verbose):
    sql_verbose(verbose, drop_table_str)
    try:
        if drop_table_str.endswith(".csv"):
            os.remove(drop_table_str)
        else:
            os.remove(drop_table_str + ".csv")
        print(f"{drop_table_str} table dropped.")
    except Exception as e:              # https://docs.python.org/3/tutorial/errors.html
        print(e)
        sys.exit('File Not Found')    # Assume any error here would be file not found

def main():
    sql_parse()


if __name__ == "__main__":
    main()
