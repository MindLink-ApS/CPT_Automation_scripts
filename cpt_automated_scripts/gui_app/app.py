import FreeSimpleGUI as sg
import subprocess
import os
import threading
import queue
import sys  # <--- NEW: Import the sys module

# CONSTANT: Maximum execution time in seconds (10 minutes)
MAX_EXECUTION_TIME = 600 

# --- PATH RESOLUTION FIX FOR PYINSTALLER ---
if getattr(sys, 'frozen', False):
    # Running inside a PyInstaller bundle (executable).
    # The folders were included using --add-data, and they land at the root 
    # of the temporary execution path: sys._MEIPASS.
    ROOT = sys._MEIPASS
else:
    # Running as a regular Python script (development mode).
    # This path calculation assumes the script files are located correctly 
    # relative to the running script (app.py) in the development environment.
    # We maintain the original structure's calculation for compatibility 
    # outside of the bundle.
    ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
# -------------------------------------------


# Mapping buttons to script paths with descriptions
SCRIPTS = {
    "Fair Health Facility": {
        "path": "Fair_Health_Facility/main.py",
        "desc": "Process Fair Health Facility data",
        "icon": "🏥"
    },
    "Fair Health Physicians": {
        "path": "Fair_Health_Physicians/main.py",
        "desc": "Process Fair Health Physician records",
        "icon": "👨‍⚕️"
    },
    "Medicare ASC Addenda": {
        "path": "Medicare_ASC_Addenda/main.py",
        "desc": "Process Medicare ASC Addenda files",
        "icon": "📋"
    },
    "Medicare Clinical Fees": {
        "path": "Medicare_Clinical_Fees/main.py",
        "desc": "Process Medicare Clinical Fee schedules",
        "icon": "💰"
    },
    "New Jersey DOBI": {
        "path": "New_Jersey_DOBI/main.py",
        "desc": "Process NJ Department of Banking data",
        "icon": "🏛️"
    },
    "Novitas": {
        "path": "Novitas/main.py",
        "desc": "Process Novitas Solutions data",
        "icon": "📊"
    },
}

# Set theme
sg.theme('DarkBlue3')

def run_script_with_progress(script_path, script_name, progress_queue):
    """Run script in a separate thread, capture output, and enforce a timeout."""
    try:
        process = subprocess.Popen(
            ["python", script_path], 
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        # Read output line by line (this step blocks until the script finishes writing or exits)
        for line in process.stdout:
            progress_queue.put(("output", line.strip()))
        
        # Wait for process to complete, enforcing the maximum execution time (10 minutes).
        try:
            return_code = process.wait(timeout=MAX_EXECUTION_TIME)
            
            if return_code == 0:
                progress_queue.put(("success", f"{script_name} completed successfully!"))
            else:
                stderr = process.stderr.read()
                progress_queue.put(("error", f"Script failed with exit code {return_code}:\n{stderr}"))
                
        except subprocess.TimeoutExpired:
            # If the process is still running after MAX_EXECUTION_TIME, kill it.
            process.kill()
            process.wait() # Wait for termination
            progress_queue.put(("error", f"Script **TIMED OUT** after {MAX_EXECUTION_TIME / 60} minutes and was forcibly terminated."))


    except Exception as e:
        progress_queue.put(("error", f"Exception occurred during execution setup: {str(e)}"))

def show_progress_window(script_name, script_path):
    """Show progress window with real-time updates"""
    
    progress_layout = [
        [sg.Text(f'🔄 Running: {script_name}', font=("Helvetica", 12, "bold"), pad=(20, 20), justification='center', expand_x=True)],
        [sg.Text('Pipeline Steps:', font=("Helvetica", 10), pad=(20, 20))],
        [sg.Text('⚪ Scraping data...', key='-STEP1-', font=("Helvetica", 10), size=(40, 1), pad=(30, 5))],
        [sg.Text('⚪ Cleaning and processing...', key='-STEP2-', font=("Helvetica", 10), size=(40, 1), pad=(30, 5))],
        [sg.Text('⚪ Inserting into database...', key='-STEP3-', font=("Helvetica", 10), size=(40, 1), pad=(30, 5))],
        [sg.ProgressBar(100, orientation='h', size=(40, 20), key='-PROGRESS-', pad=(20, 20), bar_color=('#2E86AB', '#D3D3D3'))],
        [sg.Text(f'Initializing... (Max time: {MAX_EXECUTION_TIME / 60} min)', key='-STATUS-', font=("Helvetica", 9), text_color='lightgray', justification='center', expand_x=True, pad=(20, 10))],
        [sg.Button('Cancel', button_color=('white', '#C1121F'), size=(12, 1), pad=(20, 20))],
    ]
    
    progress_window = sg.Window('Pipeline Progress', 
                                progress_layout, 
                                finalize=True, 
                                keep_on_top=True,
                                modal=True)
    
    # Create queue for thread communication
    progress_queue = queue.Queue()
    
    # Start script in separate thread
    script_thread = threading.Thread(
        target=run_script_with_progress,
        args=(script_path, script_name, progress_queue),
        daemon=True
    )
    script_thread.start()
    
    # Progress tracking
    progress = 0
    current_step = 0
    
    while True:
        event, values = progress_window.read(timeout=100)
        
        if event in (sg.WINDOW_CLOSED, 'Cancel'):
            progress_window.close()
            return False
        
        # Check for updates from the script thread
        try:
            while True:
                msg_type, msg_content = progress_queue.get_nowait()
                
                if msg_type == "output":
                    # Update progress based on keywords in output
                    msg_lower = msg_content.lower()
                    
                    if ('scrap' in msg_lower or 'fetch' in msg_lower or 'download' in msg_lower) and current_step < 1:
                        current_step = 1
                        progress = 33
                        progress_window['-STEP1-'].update('🔵 Scraping data...')
                        progress_window['-STATUS-'].update('🌐 Fetching data from sources...')
                        
                    elif ('clean' in msg_lower or 'process' in msg_lower or 'transform' in msg_lower) and current_step < 2:
                        current_step = 2
                        progress = 66
                        progress_window['-STEP1-'].update('✅ Scraping data...')
                        progress_window['-STEP2-'].update('🔵 Cleaning and processing...')
                        progress_window['-STATUS-'].update('🧹 Processing and cleaning data...')
                        
                    elif ('insert' in msg_lower or 'sav' in msg_lower or 'database' in msg_lower or 'commit' in msg_lower) and current_step < 3:
                        current_step = 3
                        progress = 90
                        progress_window['-STEP1-'].update('✅ Scraping data...')
                        progress_window['-STEP2-'].update('✅ Cleaning and processing...')
                        progress_window['-STEP3-'].update('🔵 Inserting into database...')
                        progress_window['-STATUS-'].update('💾 Saving to database...')
                    
                    progress_window['-PROGRESS-'].update(progress)
                
                elif msg_type == "success":
                    progress_window['-STEP1-'].update('✅ Scraping data...')
                    progress_window['-STEP2-'].update('✅ Cleaning and processing...')
                    progress_window['-STEP3-'].update('✅ Inserting into database...')
                    progress_window['-PROGRESS-'].update(100)
                    progress_window['-STATUS-'].update('✓ All steps completed successfully!')
                    progress_window.refresh()
                    
                    # Wait a moment to show completion
                    sg.Window.read(progress_window, timeout=1500)
                    progress_window.close()
                    
                    sg.popup("✓ Success", 
                            msg_content,
                            font=("Helvetica", 11),
                            button_color=('white', '#2E7D32'),
                            keep_on_top=True)
                    return True
                
                elif msg_type == "error":
                    progress_window.close()
                    sg.popup_error("✗ Error", 
                                  msg_content,
                                  font=("Helvetica", 11),
                                  keep_on_top=True)
                    return False
                    
        except queue.Empty:
            # Animate progress slightly even without updates
            if current_step == 0 and progress < 30:
                progress = min(progress + 0.2, 30)
                progress_window['-PROGRESS-'].update(progress)

# Header
layout = [
    [sg.Text("Medical Data Automation Runner", 
             font=("Helvetica", 20, "bold"), 
             justification='center', 
             expand_x=True,
             pad=(0, 20))],
    [sg.Text("Select a pipeline to execute", 
             font=("Helvetica", 10), 
             text_color='lightgray',
             justification='center',
             expand_x=True,
             pad=(0, (0, 20)))],
    [sg.HorizontalSeparator(pad=(20, 10))],
]

# Add buttons for each script with improved styling
for label, info in SCRIPTS.items():
    layout.append([
        sg.Column([
            [sg.Text(info['icon'], font=("Helvetica", 16), pad=(0, 0))],
        ], pad=(10, 0)),
        sg.Column([
            [sg.Text(label, font=("Helvetica", 11, "bold"))],
            [sg.Text(info['desc'], font=("Helvetica", 9), text_color='lightgray')],
        ], expand_x=True, pad=(10, 0)),
        sg.Button("Run", 
                  key=label,
                  size=(10, 2),
                  button_color=('white', '#2E86AB'),
                  mouseover_colors=('white', '#1a5f7a'),
                  font=("Helvetica", 10, "bold"),
                  border_width=0,
                  pad=(10, 5))
    ])
    layout.append([sg.HorizontalSeparator(pad=(20, 5))])

# Footer with Exit button
layout.append([
    sg.Push(),
    sg.Button("Exit", 
              size=(15, 1),
              button_color=('white', '#C1121F'),
              mouseover_colors=('white', '#8B0000'),
              font=("Helvetica", 10, "bold"),
              border_width=0,
              pad=(0, 20)),
    sg.Push()
])

# Create window with better sizing
window = sg.Window("Medical Data Pipeline Launcher", 
                   layout,
                   size=(650, 600),
                   element_justification='left',
                   finalize=True)

while True:
    event, values = window.read()
    
    if event in (sg.WINDOW_CLOSED, "Exit"):
        break
    
    if event in SCRIPTS:
        # Use the correctly resolved ROOT variable
        script_path = os.path.join(ROOT, SCRIPTS[event]["path"]) 
        show_progress_window(event, script_path)

window.close()