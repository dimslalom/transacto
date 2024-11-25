from flask import Flask, render_template, request, send_file, jsonify
from werkzeug.utils import secure_filename
import os
import shutil
import pandas as pd
from data_lake import DataLake
from pathlib import Path
import json
import time

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024  # 16MB max file size

# Initialize Data Lake
data_lake = DataLake("data_lake")

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400
    
    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400
        
    filename = secure_filename(file.filename)
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)
    
    # Save uploaded file temporarily
    file.save(filepath)
    
    # Process file through data lake
    try:
        data_lake.copy_to_raw(filepath)
        data_lake.process_raw_data()
        data_lake.update_master_database()
        os.remove(filepath)  # Clean up temp file
        return jsonify({'message': 'File processed successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/view/<path:file_path>')
def view_data(file_path):
    try:
        if (file_path.startswith('processed/')):
            # Remove 'processed/' prefix if present
            folder = os.path.dirname(file_path[10:])
            file_name = os.path.splitext(os.path.basename(file_path))[0]
            df = data_lake.get_processed_data(folder, file_name)
            return render_template('view.html', data=df.to_html(classes='table table-striped'))
        else:
            # For raw files, show file details
            full_path = os.path.join('data_lake', file_path)
            if os.path.exists(full_path):
                with open(full_path, 'r') as f:
                    content = f.read()
                return render_template('view.html', 
                                    data=f'<pre class="bg-light p-3">{content}</pre>')
            else:
                return render_template('view.html', 
                                    data=f'<div class="alert alert-danger">Error: File not found</div>')
    except Exception as e:
        return render_template('view.html', 
                             data=f'<div class="alert alert-danger">Error: {str(e)}</div>')

@app.route('/files')
def list_files():
    files = []
    for folder in ['raw', 'processed']:
        path = os.path.join('data_lake', folder)
        for root, _, filenames in os.walk(path):
            for filename in filenames:
                files.append({
                    'name': filename,
                    'path': os.path.join(folder, filename),  # Keep folder/filename format for backend
                    'zone': folder
                })
    return jsonify(files)

@app.route('/delete/<path:file_path>', methods=['DELETE'])
def delete_file(file_path):
    try:
        # Construct full path similar to view function
        full_path = os.path.join('data_lake', file_path)
        print(f"Attempting to delete: {full_path}")  # Debug print
        
        if os.path.exists(full_path):
            os.remove(full_path)
            return jsonify({'message': f'File {file_path} deleted successfully'})
        return jsonify({'error': f'File not found: {full_path}'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/move', methods=['POST'])
def move_file():
    try:
        source = request.json.get('source')
        destination = request.json.get('destination')
        if not source or not destination:
            return jsonify({'error': 'Source and destination required'}), 400
        
        # Construct full paths similar to view function
        source_path = os.path.join('data_lake', source)
        dest_path = os.path.join('data_lake', destination)
        
        print(f"Moving from: {source_path} to: {dest_path}")  # Debug print
        
        # Ensure source exists
        if not os.path.exists(source_path):
            return jsonify({'error': f'Source file not found: {source_path}'}), 404
            
        # Create destination directory if needed
        os.makedirs(os.path.dirname(dest_path), exist_ok=True)
        
        # Move the file
        shutil.move(source_path, dest_path)
        return jsonify({'message': 'File moved successfully'})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/file/<path:file_path>')
def file_details(file_path):
    try:
        # Normalize file path
        if file_path.startswith('data_lake/'):
            file_path = file_path[10:]  # Remove data_lake/ prefix
        full_path = Path(app.root_path) / 'data_lake' / file_path
        
        if full_path.exists():
            if file_path.endswith('.json'):
                with open(full_path, 'r', encoding='utf-8') as f:
                    content = f.read()
                return render_template('view.html', 
                                    data=f'<pre class="bg-light p-3">{content}</pre>')
            # For other files, return basic info
            return jsonify({
                'name': full_path.name,
                'size': os.path.getsize(full_path),
                'modified': os.path.getmtime(full_path)
            })
        return jsonify({'error': 'File not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/transactions')
def get_transactions():
    try:
        # Initialize master database if it doesn't exist
        if not os.path.exists('data_lake/master_database.json'):
            with open('data_lake/master_database.json', 'w', encoding='utf-8') as f:
                json.dump([], f)
            print("Initialized master database")

        # Get data with retry logic
        max_retries = 3
        for attempt in range(max_retries):
            try:
                df = data_lake.get_master_data()
                break
            except Exception as e:
                if attempt == max_retries - 1:
                    raise e
                time.sleep(0.5)
                continue

        # Handle empty DataFrame case
        if df.empty:
            return jsonify({'data': '<p class="text-muted">No transactions found.</p>'})

        # Process the DataFrame
        df = df.copy()  # Create a copy to avoid SettingWithCopyWarning
        df = df.fillna('')

        # Format columns
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d')
        if 'amount' in df.columns:
            df['amount'] = df['amount'].apply(lambda x: f"{float(x):.2f}" if x != '' else '')
        if 'source_file' in df.columns:
            df['source_file'] = df['source_file'].apply(
                lambda x: ' '.join([
                    f'<a href="/file/{file.strip()}" class="btn btn-sm btn-outline-secondary">{Path(file.strip()).name}</a>' 
                    for file in str(x).split(', ')
                ])
            )

        # Ensure all required columns exist
        for col in ['date', 'amount', 'description', 'payee', 'source_file']:
            if col not in df.columns:
                df[col] = ''

        # Generate HTML table
        table_html = df.to_html(
            classes='table table-striped table-hover',
            index=False,
            table_id='transactionTable',
            escape=False
        )
        
        return jsonify({'data': table_html})

    except Exception as e:
        print(f"Error in get_transactions: {str(e)}")  # Server-side logging
        return jsonify({
            'error': f'Unable to load transactions: {str(e)}. Please try refreshing the page.'
        }), 500

@app.route('/refresh')
def refresh():
    return jsonify({'message': 'Refresh triggered'})

@app.route('/search', methods=['POST'])
def search_transactions():
    try:
        # Get search parameters with debug logging
        data = request.get_json()
        print(f"Received search request: {data}")  # Debug print
        
        search_query = data.get('query', '').strip()
        search_fields = data.get('fields', [])
        
        print(f"Search query: {search_query}, fields: {search_fields}")  # Debug print
        
        if not search_query:
            return jsonify({'data': '<p class="text-muted">Please enter a search term.</p>'})

        # Make sure master database exists
        if not os.path.exists('data_lake/master_database.json'):
            with open('data_lake/master_database.json', 'w', encoding='utf-8') as f:
                json.dump([], f)

        # Perform search
        results = data_lake.search_transactions(search_query, search_fields)
        print(f"Search results: {len(results)} rows found")  # Debug print
        
        if results.empty:
            return jsonify({'data': '<p class="text-muted">No results found.</p>'})

        # Format results
        results = results.copy()
        results = results.fillna('')

        # Format date with error handling
        if 'date' in results.columns:
            try:
                results['date'] = pd.to_datetime(results['date'], unit='ms').dt.strftime('%Y-%m-%d')
            except Exception as e:
                print(f"Error formatting dates: {e}")
                # Fallback date formatting if unit='ms' fails
                results['date'] = pd.to_datetime(results['date']).dt.strftime('%Y-%m-%d')

        # Format amount with error handling
        if 'amount' in results.columns:
            try:
                results['amount'] = results['amount'].apply(lambda x: f"{float(x):.2f}" if x != '' else '')
            except Exception as e:
                print(f"Error formatting amounts: {e}")

        # Format source files as buttons
        if 'source_file' in results.columns:
            try:
                results['source_file'] = results['source_file'].apply(
                    lambda x: ' '.join([
                        f'<a href="/file/{file.strip()}" class="btn btn-sm btn-outline-secondary">{Path(file.strip()).name}</a>' 
                        for file in str(x).split(', ')
                    ])
                )
            except Exception as e:
                print(f"Error formatting source files: {e}")

        # Ensure all required columns exist
        required_columns = ['date', 'amount', 'description', 'payee', 'source_file']
        for col in required_columns:
            if col not in results.columns:
                results[col] = ''

        # Generate table HTML
        table_html = results.to_html(
            classes='table table-striped table-hover',
            index=False,
            table_id='transactionTable',
            escape=False
        )
        
        return jsonify({'data': table_html})

    except Exception as e:
        print(f"Search error: {str(e)}")  # Detailed error logging
        return jsonify({'error': str(e)}), 500

@app.route('/entry/<int:timestamp>', methods=['GET'])
def get_entry(timestamp):
    try:
        entry = data_lake.get_entry(timestamp)
        if entry is not None:
            return jsonify(entry)
        return jsonify({'error': 'Entry not found'}), 404
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/entry', methods=['POST'])
def add_entry():
    try:
        entry_data = request.json
        success = data_lake.add_entry(entry_data)
        if success:
            return jsonify({'message': 'Entry added successfully'})
        return jsonify({'error': 'Failed to add entry'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/entry/<int:timestamp>', methods=['PUT'])
def update_entry(timestamp):
    try:
        entry_data = request.json
        success = data_lake.update_entry(timestamp, entry_data)
        if success:
            return jsonify({'message': 'Entry updated successfully'})
        return jsonify({'error': 'Failed to update entry'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/entry/<int:timestamp>', methods=['DELETE'])
def delete_entry(timestamp):
    try:
        success = data_lake.delete_entry(timestamp)
        if success:
            return jsonify({'message': 'Entry deleted successfully'})
        return jsonify({'error': 'Failed to delete entry'}), 400
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/entries/delete-multiple', methods=['POST'])
def delete_multiple_entries():
    try:
        timestamps = request.json.get('timestamps', [])
        if not timestamps:
            return jsonify({'error': 'No entries selected'}), 400

        success_count = 0
        for timestamp in timestamps:
            if data_lake.delete_entry(int(timestamp)):
                success_count += 1

        return jsonify({
            'message': f'Successfully deleted {success_count} out of {len(timestamps)} entries'
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
    app.run(debug=True)