import os
from flask import Flask, jsonify, render_template
import pyodbc
from dotenv import load_dotenv
import logging
import pandas as pd
from datetime import datetime, timedelta, time
from flask_mail import Mail, Message
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import atexit

# Load environment variables from .env file
load_dotenv()

# Configure logging
logging.basicConfig(filename='app.log', level=logging.INFO,
                    format='%(asctime)s %(levelname)s:%(message)s')

app = Flask(__name__)

# Email configuration
app.config['MAIL_SERVER'] = os.getenv('MAIL_SERVER')
app.config['MAIL_PORT'] = int(os.getenv('MAIL_PORT', 587))
app.config['MAIL_USE_TLS'] = os.getenv('MAIL_USE_TLS', 'True') == 'True'
app.config['MAIL_USERNAME'] = os.getenv('MAIL_USERNAME')
app.config['MAIL_PASSWORD'] = os.getenv('MAIL_PASSWORD')
mail = Mail(app)

# Database configuration from environment variables
DRIVER = os.getenv('DB_DRIVER', '{SQL Server}')
SERVER = os.getenv('DB_SERVER', 'z7\\mage800,74951')
DATABASE = os.getenv('DB_DATABASE', 'kzstock')
USERNAME = os.getenv('DB_USERNAME', 'gek')
PASSWORD = os.getenv('DB_PASSWORD', 'Flag88')

# Shift timings data with shift IDs
shift_timings = [
    {"NAME": "Dave", "USERNAME": "DAVE M2", "START_TIME": "7:00 AM", "END_TIME": "3:30 PM", "SHIFT": "Shift 1"},
    {"NAME": "Jamie Richard ", "USERNAME": "JamieRic", "START_TIME": "7:00 AM", "END_TIME": "3:30 PM", "SHIFT": "Shift 1"},
    {"NAME": "Leo", "USERNAME": "LEO", "START_TIME": "7:00 AM", "END_TIME": "3:30 PM", "SHIFT": "Shift 1"},
    {"NAME": "Kieron", "USERNAME": "KIERONS2", "START_TIME": "7:00 AM", "END_TIME": "3:30 PM", "SHIFT": "Shift 1"},
    {"NAME": "Mike", "USERNAME": "MIKEK2", "START_TIME": "7:00 AM", "END_TIME": "3:30 PM", "SHIFT": "Shift 1"},
    {"NAME": "Steve", "USERNAME": "STEVE.", "START_TIME": "7:00 AM", "END_TIME": "3:30 PM", "SHIFT": "Shift 1"},
    {"NAME": "Reece", "USERNAME": "REECE", "START_TIME": "7:00 AM", "END_TIME": "3:30 PM", "SHIFT": "Shift 1"},
    {"NAME": "Jay", "USERNAME": "Jay Richmond", "START_TIME": "7:00 AM", "END_TIME": "3:30 PM", "SHIFT": "Shift 1"},
    {"NAME": "Liam", "USERNAME": "Liam", "START_TIME": "9:00 AM", "END_TIME": "5:30 PM", "SHIFT": "Shift 2"},
    {"NAME": "Ross", "USERNAME": "Ross", "START_TIME": "9:00 AM", "END_TIME": "5:30 PM", "SHIFT": "Shift 2"},
    {"NAME": "Cameron", "USERNAME": "cameron", "START_TIME": "9:00 AM", "END_TIME": "5:30 PM", "SHIFT": "Shift 2"},
    {"NAME": "Oliver", "USERNAME": "oliver", "START_TIME": "11:00 AM", "END_TIME": "7:30 PM", "SHIFT": "Shift 3"},
    {"NAME": "Tommo jnr", "USERNAME": "Tommo jnr", "START_TIME": "11:00 AM", "END_TIME": "7:30 PM", "SHIFT": "Shift 3"},
    {"NAME": "Ryan", "USERNAME": "Ryan Nixon", "START_TIME": "11:00 AM", "END_TIME": "7:30 PM", "SHIFT": "Shift 3"},
    {"NAME": "Andy", "USERNAME": "andy stock", "START_TIME": "11:00 AM", "END_TIME": "7:30 PM", "SHIFT": "Shift 3"},
    {"NAME": "Tom", "USERNAME": "T0M", "START_TIME": "7:00 AM", "END_TIME": "3:30 PM", "SHIFT": "Shift 1"},
    {"NAME": "Alan Weston", "USERNAME": "Allan", "START_TIME": "11:00 AM", "END_TIME": "7:30 PM", "SHIFT": "Shift 3"},
     
]

@app.route('/')
def home():
    return render_template('sintex.html')

def fetch_performance_data():
    try:
        logging.info("Fetching pick performance data from database.")

        # Convert shift timings to DataFrame
        shift_df = pd.DataFrame(shift_timings)

        # Standardize usernames by lowercasing and stripping
        shift_df['USERNAME_lower'] = shift_df['USERNAME'].str.lower().str.strip()

        # Convert shift times to datetime.time objects with error handling
        shift_df['START_TIME'] = pd.to_datetime(shift_df['START_TIME'], format='%I:%M %p', errors='coerce').dt.time
        shift_df['END_TIME'] = pd.to_datetime(shift_df['END_TIME'], format='%I:%M %p', errors='coerce').dt.time

        # Check for any parsing errors
        if shift_df['START_TIME'].isnull().any() or shift_df['END_TIME'].isnull().any():
            logging.error("Error parsing shift times. Please check the shift timings format.")
            return None

        # Expected pick time in seconds
        expected_pick_time = 135  # 2 minutes and 15 seconds

        # Establish a new connection for each request
        with pyodbc.connect(
            f'DRIVER={DRIVER};SERVER={SERVER};DATABASE={DATABASE};UID={USERNAME};PWD={PASSWORD}'
        ) as conn:
            query = """
                SELECT
                    [PickedBy],
                    [PickedDateTime],
                    [Qty]
                FROM
                    [BLStock].[dbo].[GDNLocations]
                WHERE
                    [PickedDateTime] >= ? AND [PickedDateTime] < ?
                ORDER BY
                    [PickedBy], [PickedDateTime]
            """
            # Fetch data for the current day
            today = datetime.now().date()
            start_of_day = datetime.combine(today, time.min)
            end_of_day = datetime.combine(today + timedelta(days=1), time.min)

            params = (start_of_day, end_of_day)
            df = pd.read_sql(query, conn, params=params)

        if df.empty:
            logging.warning("No pick data found for today.")
            return pd.DataFrame()  # Return empty DataFrame

        # Process the data
        df['PickedDateTime'] = pd.to_datetime(df['PickedDateTime'])
        df['PickedBy'] = df['PickedBy'].str.strip()

        # Standardize PickedBy for merging
        df['PickedBy_lower'] = df['PickedBy'].str.lower().str.strip()

        # Merge pick data with shift timings on lowercased usernames
        df = df.merge(
            shift_df[['USERNAME_lower', 'NAME', 'START_TIME', 'END_TIME', 'SHIFT']],
            left_on='PickedBy_lower',
            right_on='USERNAME_lower',
            how='left'
        )

        # Drop helper columns
        df.drop(columns=['PickedBy_lower', 'USERNAME_lower'], inplace=True)

        # Log if any shifts are missing
        if df['SHIFT'].isnull().any():
            missing_shifts = df[df['SHIFT'].isnull()]
            logging.warning(f"Picks with missing shift information: {missing_shifts['PickedBy'].unique()}")

        # Filter out picks where shift information is missing
        df = df.dropna(subset=['START_TIME', 'END_TIME', 'SHIFT', 'NAME'])

        # Check if df is empty after merging and filtering
        if df.empty:
            logging.warning("No pick data found within shift timings.")
            return pd.DataFrame()

        # Function to check if pick is within user's shift
        def is_within_shift(row):
            shift_date = row['PickedDateTime'].date()
            shift_start = datetime.combine(shift_date, row['START_TIME'])
            shift_end = datetime.combine(shift_date, row['END_TIME'])
            return shift_start <= row['PickedDateTime'] <= shift_end

        # Apply the function to filter the DataFrame
        df = df[df.apply(is_within_shift, axis=1)]

        # Sort and calculate time differences
        df.sort_values(by=['PickedBy', 'PickedDateTime'], inplace=True)
        df['TimeDiff'] = df.groupby('PickedBy')['PickedDateTime'].diff().dt.total_seconds()

        # Calculate performance metrics
        df['PerformanceRatio'] = df['TimeDiff'] / expected_pick_time
        df['PerformanceRatio'] = df['PerformanceRatio'].fillna(1)

        # Group by NAME to get aggregate metrics
        metrics = df.groupby('NAME').agg({
            'TimeDiff': ['mean', 'median'],
            'PerformanceRatio': 'mean',
            'Qty': 'sum',  # Sum of quantities picked
            'START_TIME': 'first',
            'END_TIME': 'first',
            'SHIFT': 'first'
        }).reset_index()

        # Rename columns to reflect 'actualpicked'
        metrics.columns = ['NAME', 'AvgTimeDiff', 'MedianTimeDiff', 'AvgPerformanceRatio', 'actualpicked', 'START_TIME', 'END_TIME', 'SHIFT']

        # Calculate Expected Picks based on elapsed shift time
        def calculate_expected_picks(row):
            current_datetime = datetime.now()
            shift_date = current_datetime.date()
            shift_start = datetime.combine(shift_date, row['START_TIME'])
            shift_end = datetime.combine(shift_date, row['END_TIME'])

            # Stop calculation if the shift has ended
            if current_datetime >= shift_end:
                elapsed_shift_duration_seconds = (shift_end - shift_start).total_seconds()
            else:
                elapsed_shift_duration_seconds = (current_datetime - shift_start).total_seconds()

            expected_picks = elapsed_shift_duration_seconds / expected_pick_time
            return expected_picks

        # Apply the function to calculate expected picks
        metrics['ExpectedPicks'] = metrics.apply(calculate_expected_picks, axis=1)

        # Calculate Performance Percentage
        metrics['PerformancePercentage'] = (metrics['actualpicked'] / metrics['ExpectedPicks']) * 100

        # Round numeric values for better readability
       
        metrics['AvgPerformanceRatio'] = metrics['AvgPerformanceRatio'].round(2) 
        metrics['ExpectedPicks'] = metrics['ExpectedPicks'].round(2)
        metrics['PerformancePercentage'] = metrics['PerformancePercentage'].round(2)

        # Convert time columns to string for JSON serialization
        metrics['START_TIME'] = metrics['START_TIME'].astype(str)
        metrics['END_TIME'] = metrics['END_TIME'].astype(str)
        metrics_for_email = metrics.drop(columns=['AvgTimeDiff', 'MedianTimeDiff'])

        logging.info("Fetched and processed performance data successfully.")
        return metrics_for_email  # Return the modified metrics for the email


    except pyodbc.Error as db_err:
        logging.error(f"Database error: {db_err}")
        return None
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return None


def send_email_report():
    logging.info("Preparing to send email report.")

    metrics = fetch_performance_data()

    if metrics is None:
        logging.error("Failed to fetch performance data. Email not sent.")
        return

    if metrics.empty:
        logging.warning("No data to send in the report.")
        return

    # Create a report, e.g., as a CSV attachment
    report_csv = metrics.to_csv(index=False)

    # Create the email message
    msg = Message(
        subject="Daily Pick Performance Report",
        sender=app.config['MAIL_USERNAME'],
        recipients=[os.getenv('MAIL_RECIPIENT')]
    )

    msg.body = "Please find attached the daily pick performance report."

    # Attach the CSV report
    msg.attach(
        filename="pick_performance_report.csv",
        content_type="text/csv",
        data=report_csv
    )

    try:
        mail.send(msg)
        logging.info("Email report sent successfully.")
    except Exception as e:
        logging.error(f"Failed to send email report: {e}")

def schedule_email_report():
    scheduler = BackgroundScheduler()
    trigger = CronTrigger(hour=1, minute=0)  # 1:00 AM every day
    scheduler.add_job(func=send_email_report, trigger=trigger, id='daily_email_report')
    scheduler.start()
    logging.info("Scheduler started for daily email report at 1:00 AM.")

# Initialize the scheduler when the app starts
schedule_email_report()

# Ensure the scheduler shuts down when the app stops
def shutdown_scheduler():
    scheduler = BackgroundScheduler()
    if scheduler.running:
        scheduler.shutdown()
        logging.info("Scheduler shut down successfully.")

atexit.register(shutdown_scheduler)

@app.route('/send-test-email')
def send_test_email():
    send_email_report()
    return "Test email sent!"


@app.route('/pick-performance', methods=['GET'])
def pick_performance():
    try:
        logging.info("Fetching pick performance data for API request.")

        metrics = fetch_performance_data()

        if metrics is None:
            return jsonify({"status": "error", "message": "Failed to fetch performance data."}), 500

        if metrics.empty:
            return jsonify({"status": "success", "data": []})

        # Convert to list of dictionaries
        data = metrics.to_dict(orient='records')

        logging.info("API request for pick performance data successful.")
        return jsonify({"status": "success", "data": data})

    except Exception as e:
        logging.error(f"Unexpected error in /pick-performance route: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    debug_mode = os.getenv('FLASK_DEBUG', 'False').lower() in ['true', '1', 't']
    app.run(debug=debug_mode, port=5001)
