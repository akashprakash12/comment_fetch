import requests
import pandas as pd
import json
from datetime import datetime, timezone
import time
import os
import urllib.parse

# Configuration
page_id = '61576451374046'  # Replace with your Facebook Page ID
access_token = 'EAAWySti66ogBOZCXuOzanKPC6ZB3BJNCIZB7mfL8FJxZCFOHgcqQFeovWOp9BO5GsvXguYFoef65RexlBV5NJmacfqSK12zvxxGISRKZBOZBQopDHYfa6AWwD0HzfAeRno3Ax0FxZCiPaz4MMr8WZCvXt2XBvX0IgCAZACnMXjILWGKZA0tZAKSO4ZCiZB0yPXNfpbYxZBcEYY7zYd'  # Replace with your Facebook access token
data_file = os.path.join(os.getcwd(), 'facebook_posts_comments.xlsx')
poll_interval = 60  # seconds between checks (recommended: 60+ to avoid rate limits)

def get_all_posts():
    """Retrieve all posts from the Facebook page"""
    base_url = f'https://graph.facebook.com/v19.0/{page_id}/posts'
    
    params = {
        'fields': 'id,created_time,message,permalink_url',
        'access_token': access_token,
        'limit': 100  # Maximum allowed by Facebook API
    }
    
    all_posts = []
    next_page = True
    
    try:
        while next_page:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data:
                all_posts.extend(data['data'])
                print(f"Retrieved {len(data['data'])} posts")
            
            # Check for next page
            if 'paging' in data and 'next' in data['paging']:
                base_url = data['paging']['next']
                params = {}  # Reset params as they're included in the next URL
            else:
                next_page = False
                
    except requests.exceptions.RequestException as e:
        print(f"Error fetching posts: {e}")
        if hasattr(e, 'response') and e.response:
            print(f"Response content: {e.response.text}")
    
    return all_posts

def get_post_comments(post_id):
    """Retrieve all comments for a specific post"""
    base_url = f'https://graph.facebook.com/v19.0/{post_id}/comments'
    
    params = {
        'fields': 'id,created_time,from{name},message',
        'access_token': access_token,
        'limit': 100,
        'order': 'chronological'
    }
    
    all_comments = []
    next_page = True
    
    try:
        while next_page:
            response = requests.get(base_url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if 'data' in data:
                all_comments.extend(data['data'])
                print(f"Retrieved {len(data['data'])} comments for post {post_id}")
            
            # Check for next page
            if 'paging' in data and 'next' in data['paging']:
                base_url = data['paging']['next']
                params = {}  # Reset params as they're included in the next URL
            else:
                next_page = False
                
    except requests.exceptions.RequestException as e:
        print(f"Error fetching comments for post {post_id}: {e}")
    
    return all_comments

def process_data(posts, comments_dict):
    """Process all data into a structured format for Excel"""
    processed_data = []
    
    for post in posts:
        post_id = post.get('id', '')
        post_message = post.get('message', '[No text]')
        post_time = post.get('created_time', '')
        post_url = post.get('permalink_url', '')
        
        # Add post as a row
        processed_data.append({
            'Type': 'Post',
            'ID': post_id,
            'Author': 'Page',
            'Time': post_time,
            'Content': post_message,
            'URL': post_url,
            'Parent ID': '',
            'Parent Content': ''
        })
        
        # Add all comments for this post
        for comment in comments_dict.get(post_id, []):
            processed_data.append({
                'Type': 'Comment',
                'ID': comment.get('id', ''),
                'Author': comment.get('from', {}).get('name', 'Unknown'),
                'Time': comment.get('created_time', ''),
                'Content': comment.get('message', '[No text]'),
                'URL': f"{post_url}?comment_id={comment.get('id', '')}",
                'Parent ID': post_id,
                'Parent Content': post_message[:50] + '...' if post_message else ''
            })
    
    return processed_data

def save_to_excel(data):
    """Save all data to Excel file"""
    if not data:
        print("No data to save")
        return
    
    df = pd.DataFrame(data)
    
    # Convert time to readable format
    df['Time'] = pd.to_datetime(df['Time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(data_file) or '.', exist_ok=True)
        
        # Save to Excel
        df.to_excel(data_file, index=False, engine='openpyxl')
        print(f"Successfully saved data to: {os.path.abspath(data_file)}")
    except PermissionError:
        print(f"Error: Permission denied for file {data_file}. Is it open in another program?")
    except Exception as e:
        print(f"Error saving to Excel: {str(e)}")

def main():
    print("Starting Facebook posts and comments scraper...")
    print(f"Data will be saved to: {data_file}")
    
    try:
        # Step 1: Get all posts
        print("\nFetching all posts from the page...")
        posts = get_all_posts()
        
        if not posts:
            print("No posts found or error occurred")
            return
        
        # Step 2: Get comments for each post
        print("\nFetching comments for each post...")
        comments_dict = {}
        for post in posts:
            post_id = post.get('id', '')
            if post_id:
                comments = get_post_comments(post_id)
                comments_dict[post_id] = comments
            time.sleep(1)  # Small delay to avoid rate limiting
        
        # Step 3: Process and save data
        print("\nProcessing and saving data...")
        processed_data = process_data(posts, comments_dict)
        save_to_excel(processed_data)
        
        print("\nCompleted successfully!")
        print(f"Total posts: {len(posts)}")
        print(f"Total comments: {sum(len(comments) for comments in comments_dict.values())}")
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    # Verify dependencies
    try:
        import openpyxl
    except ImportError:
        print("Error: Required package 'openpyxl' is not installed.")
        print("Please install it with: pip install openpyxl")
        exit(1)
    
    main()