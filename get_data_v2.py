import os
import logging
import argparse
import numpy as np
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup

# Configure logging for better traceability
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def user_tag_extract(tag):
    """
    Function to check if a tag is an anchor (<a>) with a 'target' attribute.
    """
    return tag.name == 'a' and tag.has_attr('target')

def read_file(file_path):
    """
    Reads the contents of the file at the given file path.

    Args:
        file_path (str): Path to the HTML file to read.

    Returns:
        str: File content as a string, or None if the file could not be read.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            return file.read()
    except FileNotFoundError:
        logging.error(f"File '{file_path}' not found.")
    except IOError as e:
        logging.error(f"Error reading file '{file_path}': {e}")
    return None

def extract_user_links(html_content):
    """
    Extracts content of all <a> tags with a 'target' attribute from the given HTML content.

    Args:
        html_content (str): HTML content to parse.

    Returns:
        list: A list of the contents of <a> tags with 'target' attribute, or empty list if none found.
    """
    if not html_content:
        logging.warning("No HTML content provided for parsing.")
        return []

    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        elem = [e.contents[0] for e in soup.find_all(user_tag_extract)]
        return elem
    except Exception as e:
        logging.error(f"Error parsing HTML content: {e}")
        return []

def update_csv_with_new_data(file_path, new_data_df, existing_df=None, unique_column='following_me_only'):
    """
    Updates a CSV file with new rows that do not already exist, based on a unique identifier column.
    
    Args:
        file_path (str): Path to the CSV file.
        new_data_df (pandas.DataFrame): DataFrame containing new data to add.
        existing_df (pandas.DataFrame, optional): Existing DataFrame if reading from an existing file. Default is None.
        unique_column (str): The column used to check for duplicates (default is 'following_me_only').
    
    Returns:
        None
    """
    if existing_df is None:
        existing_df = pd.DataFrame(columns=new_data_df.columns)  # Empty DataFrame with same columns if none provided
        
    # Identify rows that are not already in the existing DataFrame based on the unique column
    new_rows = new_data_df[~new_data_df[unique_column].isin(existing_df[unique_column])]
    
    # Append the new rows to the existing DataFrame
    if not new_rows.empty:
        updated_df = existing_df.append(new_rows, ignore_index=True)
        updated_df.to_csv(file_path, index=False)
        print(f"File updated: {file_path}")
    else:
        print(f"No new data to update in: {file_path}")

def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(description="Extract targeted <a> tag contents from an HTML file.")
    parser.add_argument('--followers-list', type=str, required=True, help="Path to the followers HTML file.")
    parser.add_argument('--following-list', type=str, required=True, help="Path to the following HTML file.")
    
    # Parse the arguments
    args = parser.parse_args()

    # Use the file paths passed from the command line
    following_html_path = args.following_list
    followers_html_path = args.followers_list
    
    # Read file content
    logging.info(f"Reading files \n'{followers_html_path}' and '{following_html_path}'...")
    html_content_followers = read_file(followers_html_path)
    html_content_following = read_file(following_html_path)

    if html_content_followers and html_content_following:
        # Extract links with a 'target' attribute
        logging.info("Extracting user links...")
        extracted_data_following = extract_user_links(html_content_following)
        extracted_data_followers = extract_user_links(html_content_followers)

        if extracted_data_followers and extracted_data_following:
            logging.info(f"Extracted {len(extracted_data_followers)} users from followers and {len(extracted_data_following)} users from following.")
            
            # Compute the difference between following and followers
            following_only = np.setdiff1d(extracted_data_following, extracted_data_followers)
            followers_only = np.setdiff1d(extracted_data_followers, extracted_data_following)
            
            # Log the differences
            logging.info(f"Users in 'following' but not 'followers': {len(following_only)}")
            logging.info(f"Users in 'followers' but not 'following': {len(followers_only)}")
            
            # Current date and time
            current_datetime = datetime.now()
            formatted_datetime = current_datetime.strftime('%Y-%m-%d %H:%M:%S')
            
            # Make dfs for following and follower only
            following_only_data = {'following_them_only': following_only, 'date_added': formatted_datetime}
            following_only_df = pd.DataFrame(following_only_data)
            follower_only_data = {'following_me_only': followers_only, 'date_added': formatted_datetime}
            follower_only_df = pd.DataFrame(follower_only_data)
            
            # File paths
            follower_only_path = os.path.join(os.getcwd(), 'follower_only.csv')
            following_only_path = os.path.join(os.getcwd(), 'following_only.csv')
            stopped_following_path = os.path.join(os.getcwd(), 'stopped_following_only.csv')
            deleted_or_blocked_path = os.path.join(os.getcwd(), 'deleted_or_blocked.csv')
            
            # Read existing CSV files for comparison, if they exist
            if os.path.exists(following_only_path):
                existing_following_df = pd.read_csv(following_only_path)
            else:
                # ensure correct structure
                existing_following_df = pd.DataFrame(columns=['following_them_only', 'date_added'])

            if os.path.exists(follower_only_path):
                existing_follower_df = pd.read_csv(follower_only_path)
            else:
                # ensure correct structure
                existing_follower_df = pd.DataFrame(columns=['following_me_only', 'date_added'])

            # Check for stopped following (users who were in the previous "following" but aren't now)
            # TODO: will not work... need new following and old following/etc. but that is the job of a fuller database
            # or more CSV's. But I'm not going to tdo that right now
            # stopped_following = np.setdiff1d(existing_following_df['following_me_only'], following_only)

            # Check for deleted or blocked accounts (users who were in the previous "followers" but aren't now in "following")
            # deleted_or_blocked = np.setdiff1d(existing_follower_df['following_me_only'], extracted_data_following)
            # deleted_or_blocked = np.setdiff1d(deleted_or_blocked, extracted_data_followers)

            # Combine stopped_following and deleted_or_blocked into their respective DataFrames
            # stopped_following_data = {'stopped_following': stopped_following, 'date_added': formatted_datetime}
            # stopped_following_df = pd.DataFrame(stopped_following_data)

            # deleted_or_blocked_data = {'deleted_or_blocked': deleted_or_blocked, 'date_added': formatted_datetime}
            # deleted_or_blocked_df = pd.DataFrame(deleted_or_blocked_data)

            # # Log the "stopped following" and "deleted/blocked" users
            # if stopped_following.size > 0:
            #     logging.info(f"Stopped following: {stopped_following}")
            # if deleted_or_blocked.size > 0:
            #     logging.info(f"Deleted or blocked: {deleted_or_blocked}")
            
            # Update the CSV files with new data (write once after all calculations)
            update_csv_with_new_data(following_only_path, following_only_df, existing_df=existing_following_df)
            update_csv_with_new_data(follower_only_path, follower_only_df, existing_df=existing_follower_df)
            
            # Print the differences
            if following_only.size > 0:
                logging.info(f"Following-only users: {following_only}")
            if followers_only.size > 0:
                logging.info(f"Followers-only users: {followers_only}")
        else:
            logging.info("No user links found.")
    else:
        logging.error("Failed to read the HTML file, exiting.")

if __name__ == "__main__":
    main()
