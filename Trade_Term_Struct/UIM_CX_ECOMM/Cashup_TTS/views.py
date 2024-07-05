import os
import pandas as pd
import base64
import io
import mysql.connector
from django.http import JsonResponse
from django.utils import timezone
from google.cloud import storage
from rest_framework.decorators import api_view
from datetime import timedelta,datetime


#email functionality

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime



trans_on = datetime.now()


def send_email(user):
    body = f"""
                <html>
                <head></head>
                <body>
                    <p>Dear {user},</p>
                    <br>
                    <p>Your MRP file  has been successfully uploaded on <b>{trans_on}</b>.</p>
                    <br>
                    <p>Best Regards,<br>Techtheos</p>
                </body>
                </html>
            """
    from_email = "auditpro@techtheos.com"
    password = "0BI9uqs6f*x5"

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = user
    msg['Subject'] = 'MRP File Upload Status'
    msg.attach(MIMEText(body, 'html'))

    server = smtplib.SMTP('smtp.office365.com', 587)
    server.starttls()
    server.login(from_email, password)
    server.sendmail(from_email, user, msg.as_string())
    server.quit()




#email func end

BUCKET_NAME = 'tts-stg'
FOLDER_NAME = 'tts_uploads/mrp'

EXPECTED_COLUMNS = ['Basepack', 'CBU Description', 'CBU', 'Sales Category', 'Current Net wt grm', 'Barcode/UPC',
                    'Offer Modality', 'Priority', 'Current Case Config', 'MRP', 'Stock']
EXPECTED_COLUMNS_1 = ['Error Remarks', 'Basepack', 'CBU Description', 'CBU', 'Sales Category', 'Current Net wt grm',
                      'Barcode/UPC', 'Offer Modality', 'Priority', 'Current Case Config', 'MRP', 'Stock']

# Define your connection parameters
DB_HOST = "10.2.0.3"
DB_USER = "ttsdb"
DB_PASSWORD = "Z.AmHH,QDm(Zv6?U"
DB_DATABASE = "tts_dev"


def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_DATABASE
        )
        return conn, conn.cursor()

    except mysql.connector.Error as e:
        raise ValueError(f"Error connecting to MySQL database: {e}")


def db_close(conn, cursor):
    try:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
    except Exception as e:
        raise ValueError(f"Error closing database connection: {e}")


def upload_to_gcs(file, filename):
    """Uploads a file to Google Cloud Storage."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(os.path.join(FOLDER_NAME, filename))
        blob.upload_from_file(file)
    except Exception as e:
        raise ValueError(f"Error uploading file to GCS: {e}")


def generate_public_url(filename):
    """Generates a public URL for downloading the file."""
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(os.path.join(FOLDER_NAME, filename))

        # Generate the public URL
        public_url = blob.public_url

        return public_url
    except Exception as e:
        raise ValueError(f"Error generating public URL: {e}")

"""
def save_uploaded_file(user, encoded_file):
    try:
        decoded_file = base64.b64decode(encoded_file)
        file_data = decoded_file.decode('utf-8')

        # Use chunking to handle large files
        chunk_size = 1000000  # Adjust based on your memory constraints and file size
        chunks = pd.read_csv(io.StringIO(file_data), chunksize=chunk_size)

        dfs = []
        for chunk in chunks:
            dfs.append(chunk)

        df = pd.concat(dfs, ignore_index=True)

        return df
    except Exception as e:
        raise ValueError(f'Error processing uploaded file: {e}')"""
        
def save_uploaded_file(user, encoded_file):
    try:
        decoded_file = base64.b64decode(encoded_file)
        file_data = decoded_file.decode('utf-8')

        # Use chunking to handle large files
        chunk_size = 1000000  # Adjust based on your memory constraints and file size
        chunks = pd.read_csv(io.StringIO(file_data), chunksize=chunk_size)

        dfs = []
        for chunk in chunks:
            # Convert columns to numeric where appropriate
            numeric_columns = ['Basepack', 'MRP', 'Current Net wt grm', 'Stock', 'Current Case Config']
            for col in numeric_columns:
                if col in chunk.columns:
                    chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

            dfs.append(chunk)

        df = pd.concat(dfs, ignore_index=True)

        return df
    except Exception as e:
        raise ValueError(f'Error processing uploaded file: {e}')



def validate_dataframe(df):
    # Validation logic for DataFrame
    if df.empty:
        raise ValueError('CSV file is empty')

    actual_columns = list(df.columns)
    unexpected_columns = [col for col in actual_columns if col not in EXPECTED_COLUMNS and col != 'Error Remarks']

    if unexpected_columns:
        raise ValueError(
            f"Unexpected columns found, Please use the correct template")
    
    """
    if any(not col.strip() for col in df.columns):
        raise ValueError("Empty column name found, please use the correct template")  """
  
    is_negative =(df['MRP'] < 0).any()
    if is_negative: 
        raise ValueError("Negative values found in MRP column.")
        
    is_negative =(df['Basepack'] < 0).any()
    if is_negative: 
        raise ValueError("Negative values found in Basepack column.")
        
    REQUIRED_COLUMNS = ['Basepack', 'MRP']
    empty_required_fields = [col for col in REQUIRED_COLUMNS if df[col].isnull().any()]
    if empty_required_fields:
        raise ValueError(f'Some required fields are empty: {empty_required_fields}')


    if not df['Basepack'].apply(lambda x: isinstance(x, int)).all():
        raise ValueError(
            "Inavlid Basepack datatype, please enter only numbers")

    if not df['MRP'].apply(lambda x: isinstance(x, (float, int))).all():
        raise ValueError(
            "Invalid Datatype in MRP column")
        
    empty_mrp = df['MRP'].apply(lambda x: pd.isnull(x) or x == '' or not x).any()
    if empty_mrp:
        raise ValueError("Invalid values found in 'MRP' column")
     
# Check for empty values in 'Basepack' column
    empty_basepack = df['Basepack'].apply(lambda x: pd.isnull(x) or x == '' or not x).any()
    if empty_basepack:
        raise ValueError("Invalid values found in 'Basepack' column")

    try:
        df['Current Net wt grm'] = pd.to_numeric(df['Current Net wt grm'], errors='coerce')
        df['MRP'] = pd.to_numeric(df['MRP'], errors='raise')  # This should raise an error if non-numeric values are found
        df['Stock'] = pd.to_numeric(df['Stock'], errors='coerce')
        df['Current Case Config'] = pd.to_numeric(df['Current Case Config'], errors='coerce')
    except ValueError as e:
        raise ValueError(f'Invalid value in DataFrame: {e}')


def save_to_database(df, user, year, month, quarter, cur, conn):
    try:
        filename = f'mrp_u_{user}_{timezone.now().strftime("%d-%m-%Y_%H%M%S")}.csv'
        file_data = df.to_csv(index=False)

        # Encode the file data as bytes
        encoded_file_data = file_data.encode('utf-8')

        # Create an in-memory file-like object
        file_obj = io.BytesIO(encoded_file_data)

        # Upload the file directly to GCS
        upload_to_gcs(file_obj, filename)

        data_to_insert = prepare_data_to_insert(df, user, quarter, month, year)

        clear_temp_data(user, cur)
        conn.commit()
        insert_data_to_temp_db(data_to_insert, cur)
        conn.commit()

        validate_temp_db(user, cur)
        conn.commit()

        error_check_response = check_failure_in_temp_db(user, cur)
        upload_signed_url = generate_public_url(filename)
        encoded_message = base64.b64encode(upload_signed_url.encode()).decode('ascii')

        if error_check_response:
            return error_check_response

        # Return success response
        find_count = """SELECT 
                        COUNT(reason_of_failure) AS error_count,
                        COUNT(*) total_count
                                FROM (
                        SELECT  reason_of_failure
                        FROM temp_tbl_mrp_master
                        WHERE uploaded_by = %s
                                    ) AS subquery;
                       """

        cur.execute(find_count, (user,))
        error_count, total_count = cur.fetchone()

        db_close(conn, cur)
        send_email (user)

        return JsonResponse({'message': 'File uploaded successfully', 'link': encoded_message, 'error_count': str(error_count), 'total_count': str(total_count), 'message_code': 200}, status=200)

    except Exception as e:
        return JsonResponse({'message': f'Error saving to database: {e}', 'message_code': 202}, status=202)


def prepare_data_to_insert(df, user, quarter, month, year):
    data_to_insert = []
    for _, row in df.iterrows():
        data_to_insert.append((
            int(row.get('Basepack')),
            row.get('CBU Description', None),
            row.get('CBU', None),
            row.get('Sales Category', None),
            row.get('Current Net wt grm', None),
            row.get('Barcode/UPC', None),
            row.get('Offer Modality', None),
            row.get('Priority', None),
            row.get('Current Case Config', None),
            row.get('MRP'),
            row.get('Stock', None),
            quarter,
            month,
            year,
            user,
            datetime.now(),
        ))

    # Replace NaN values with None
    data_to_insert = [
        tuple(None if pd.isna(value) else value for value in row)
        for row in data_to_insert
    ]

    return data_to_insert


def clear_temp_data(user, cur):
    clear_data = """DELETE FROM tts_dev.temp_tbl_mrp_master WHERE uploaded_by = %s;"""
    cur.execute(clear_data, (user,))


def insert_data_to_temp_db(data_to_insert, cur):
    insert_query = """
        INSERT INTO tts_dev.temp_tbl_mrp_master (
            basepack_code, cbu_description, cbu, sales_category, current_net_wt_grm, barcode_upc, offer_modality, priority,
            current_case_config, mrp, stock, quarter, month, year, uploaded_by, uploaded_on
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
    cur.executemany(insert_query, data_to_insert)


def validate_temp_db(user, cur):
    cur.callproc("tts_dev.validate_mrp_base", [user])


def check_failure_in_temp_db(user, cur):
    check_failures_query = """
        SELECT 1
        FROM tts_dev.temp_tbl_mrp_master
        WHERE uploaded_by = %s AND reason_of_failure IS NOT NULL
        LIMIT 1;
    """
    cur.execute(check_failures_query, (user,))
    failure_check = cur.fetchone()

    if failure_check:
        fetch_all_query = """         
            SELECT reason_of_failure, basepack_code, cbu_description, cbu, sales_category, current_net_wt_grm, barcode_upc, offer_modality, priority, 
                current_case_config, mrp, stock
            FROM tts_dev.temp_tbl_mrp_master
            WHERE uploaded_by = %s
            ORDER BY 1 DESC;
        """

        find_count = """SELECT 
                        COUNT(reason_of_failure) AS error_count,
                        COUNT(*) total_count
                                FROM (
                        SELECT  reason_of_failure
                        FROM temp_tbl_mrp_master
                        WHERE uploaded_by = %s
                                    ) AS subquery;
                       """

        cur.execute(fetch_all_query, (user,))
        all_records = cur.fetchall()
        all_columns = [col[0] for col in cur.description]

        cur.execute(find_count, (user,))
        error_count, total_count = cur.fetchone()

        if all_records:
            all_df = pd.DataFrame(all_records, columns=all_columns)
            all_df.columns = EXPECTED_COLUMNS_1
            error_filename = f'mrp_e_{user}_{timezone.now().strftime("%d-%m-%Y_%H%M%S")}.csv'
            file_data = all_df.to_csv(index=False)

            # Encode the file data as bytes
            encoded_file_data = file_data.encode('utf-8')

            # Create an in-memory file-like object
            file_obj = io.BytesIO(encoded_file_data)

            # Upload the file directly to GCS
            upload_to_gcs(file_obj, error_filename)

            # Return the signed URL for the uploaded file
            error_signed_url = generate_public_url(error_filename)
            encoded_message = base64.b64encode(error_signed_url.encode()).decode('ascii')
            return JsonResponse(
                {'message': 'Some records failed with validation', 'link': encoded_message, 'error_count': str(error_count), 'total_count': str(total_count), 'message_code': 201},
                status=201)
    return None


@api_view(['POST'])
def mrp_upload_file(request):
    username = request.data.get('username')
    year = request.data.get('year')
    month = request.data.get('month')
    quarter = request.data.get('quarter')
    file_base64 = request.data.get('file')
    user = request.data.get('email')

    if not file_base64:
        return JsonResponse({'message': 'No file uploaded', 'message_code': 202}, status=202)

    try:
        conn, cur = connect_to_mysql()
        df = save_uploaded_file(user, file_base64)
        validate_dataframe(df)

        result = save_to_database(df, user, year, month, quarter, cur, conn)

        return result

    except ValueError as ve:
        return JsonResponse({'message': str(ve), 'message_code': 202}, status=202)

    except Exception as e:
        return JsonResponse({'message': str(e), 'message_code': 202}, status=202)
    


