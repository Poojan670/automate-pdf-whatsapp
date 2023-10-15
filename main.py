import glob
import logging
import os
import re
import sqlite3
from datetime import datetime, timedelta

import pandas as pd
import pywhatkit as kit

# Enter your message here
automated_message = ""

# Configure the default logging
logging.basicConfig(filename="app.log", level=logging.DEBUG)
console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)  # Set the level to DEBUG
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
logging.getLogger('').addHandler(console_handler)


def extract_and_send_pdf_data(pdf_directory, database_file, phone_number_regex):
    conn = sqlite3.connect(database_file)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file (
            id INTEGER PRIMARY KEY,
            filename TEXT UNIQUE,
            phone_no TEXT,
            extracted_at DATETIME,
            sent_at DATETIME NULL
        )
    ''')

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS file_log (
            id INTEGER PRIMARY KEY,
            filename TEXT,
            status TEXT,
            log_time DATETIME
        )
    ''')

    for pdf_file in glob.glob(os.path.join(pdf_directory, '*.pdf')):
        filename = os.path.basename(pdf_file).strip()
        match = re.search(phone_number_regex, filename)

        if not match:
            logging.warning(f"No phone number found in {filename}")
            continue

        phone_number = match.group(1)
        extracted_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logging.debug("Phone No Extracted : " + phone_number)

        try:
            # Check if the file has been sent
            cursor.execute('''
                SELECT sent_at FROM file WHERE filename = ?
            ''', (filename,))
            sent_at = cursor.fetchone()

            if not sent_at:
                wait_time = 60  # Wait for 120 seconds
                call_time = datetime.now() + timedelta(seconds=wait_time)

                # Schedule the message with call_time greater than wait_time
                kit.sendwhatmsg(f"+{phone_number}", automated_message,
                                call_time.hour, call_time.minute + 1)
                sent_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                cursor.execute('''
                INSERT INTO file (filename, phone_no, extracted_at, sent_at)
                VALUES (?,?,?,?)
                ''', (filename, phone_number, extracted_at))

                # Insert an entry in the 'file_log' table
                cursor.execute('''
                    INSERT INTO file_log (file_id, status, log_time) 
                    VALUES (?, ?, ?)
                ''', (cursor.lastrowid, 'SENT', sent_at))

                # Update the 'sent_at' timestamp in the 'file' table
                cursor.execute('''
                                    UPDATE file SET sent_at = ? WHERE filename = ?
                                ''', (sent_at, filename))
            else:
                logging.info(f"File {filename} has already been sent to {phone_number}")
        except Exception as e:
            log_at = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            # Insert an entry in the 'file_log' table
            cursor.execute('''
                                INSERT INTO file_log (filename, status, log_time) 
                                VALUES (?, ?, ?)
                            ''', (filename, 'FAIL', log_at))
            logging.error(f"Failed to send {filename} to {phone_number}: {str(e)}")

        # Commit changes to the database
        conn.commit()

    conn.close()


def view_logs():
    # Retrieve and display logs
    with open("app.log", "r") as log_file:
        for line in log_file:
            print(line.strip())


def view_file_log_data():
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect('file_data.db')

        # Execute the SQL query to select all rows from the 'files' table
        query = "SELECT * FROM file_log"
        df = pd.read_sql_query(query, conn)

        # Display the data as a DataFrame
        print(df)

    except Exception as e:
        logging.error("Error while viewing file table data: " + str(e))
    finally:
        if conn:
            conn.close()


def view_file_table_data():
    try:
        # Connect to the SQLite database
        conn = sqlite3.connect('file_data.db')

        # Execute the SQL query to select all rows from the 'files' table
        query = "SELECT * FROM file"
        df = pd.read_sql_query(query, conn)

        # Display the data as a DataFrame
        print(df)

        logging.info("Viewing file table data.")
    except Exception as e:
        logging.error("Error while viewing file table data: " + str(e))
    finally:
        if conn:
            conn.close()


def list_all_files(pdf_directory):
    data = []
    for pdf_file in glob.glob(os.path.join(pdf_directory, '*.pdf')):
        filename = os.path.basename(pdf_file).strip()
        data.append(filename)
    [print("\n" + file + "\n") for file in data]


def main():
    pdf_directory = "./files"
    database_file = 'file_data.db'
    phone_number_regex = r'[-+]?(\d{5,})'
    list_all_files(pdf_directory)
    extract_and_send_pdf_data(pdf_directory, database_file, phone_number_regex)


if __name__ == "__main__":
    main()
