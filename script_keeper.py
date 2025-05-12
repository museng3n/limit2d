#!/usr/bin/env python
"""
Script Keeper - Keeps Your Python Scripts Running Forever
--------------------------------------------------------

A single file solution that monitors your Python scripts and ensures 
they keep running even if they exit or crash.

Features:
- Auto-restarts scripts when they complete or crash
- Plays alert sounds when disconnected for too long
- Logs all activities to a file
- Works even if internet disconnects
- Simple to use - just one file!
- UNLIMITED RESTARTS - never gives up!

Usage:
    python script_keeper.py your_script.py [arguments]
"""

import os
import sys
import time
import signal
import subprocess
import logging
import socket
import platform
import datetime
import threading

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("script_keeper.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("ScriptKeeper")

class ScriptKeeper:
    def __init__(self, script_path, script_args=None, 
                restart_delay=5, alert_threshold_minutes=30, 
                check_interval=5):
        """
        Initialize the script keeper.
        
        Args:
            script_path: Path to the Python script to run
            script_args: Arguments to pass to the script
            restart_delay: Delay in seconds between restart attempts
            alert_threshold_minutes: Time in minutes before triggering an alert
            check_interval: Interval in seconds to check if script is still running
        """
        self.script_path = script_path
        self.script_args = script_args or []
        self.restart_delay = restart_delay
        self.alert_threshold_minutes = alert_threshold_minutes
        self.check_interval = check_interval
        self.process = None
        self.restart_count = 0
        self.total_restarts = 0
        self.successful_runs = 0
        self.last_start_time = None
        self.disconnection_time = None
        self.alert_triggered = False
        
        # Runtime stats
        self.longest_runtime = 0
        self.total_runtime = 0
        self.last_restart_time = None
        self.backoff_minutes = 0
        
    def play_alert_sound(self):
        """Play an alert sound based on the operating system."""
        try:
            if platform.system() == "Windows":
                import winsound
                for _ in range(3):  # Play the sound 3 times
                    winsound.Beep(1000, 500)  # Frequency: 1000Hz, Duration: 500ms
                    time.sleep(0.3)
            else:
                # For Linux/Mac, use print with bell character
                for _ in range(3):  # Play the sound 3 times
                    sys.stdout.write('\a')
                    sys.stdout.flush()
                    time.sleep(0.5)
            
            logger.info("ALERT SOUND PLAYED - Script has been down too long!")
        except Exception as e:
            logger.error(f"Failed to play alert sound: {e}")

    def check_internet_connection(self):
        """Check if internet connection is available."""
        try:
            # Try to connect to Google's DNS server
            socket.create_connection(("8.8.8.8", 53), timeout=3)
            return True
        except OSError:
            return False

    def start_script(self):
        """Start the script as a subprocess."""
        if self.process and self.process.poll() is None:
            # Script is already running
            return True

        cmd = [sys.executable, self.script_path] + self.script_args
        
        try:
            # Start the process
            logger.info(f"Starting script: {' '.join(cmd)}")
            
            # Start the process and capture output
            self.process = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1,
                universal_newlines=True
            )
            
            # Start threads to monitor output in real-time
            threading.Thread(target=self._monitor_output, 
                            args=(self.process.stdout, "STDOUT"), 
                            daemon=True).start()
            threading.Thread(target=self._monitor_output, 
                            args=(self.process.stderr, "STDERR"), 
                            daemon=True).start()
            
            self.last_start_time = datetime.datetime.now()
            self.disconnection_time = None
            self.alert_triggered = False
            
            logger.info(f"Started process with ID: {self.process.pid}")
            
            # Reset backoff if we haven't restarted in a while
            current_time = datetime.datetime.now()
            if (self.last_restart_time is None or 
                (current_time - self.last_restart_time).total_seconds() > 3600):
                if self.backoff_minutes > 0:
                    logger.info(f"Resetting backoff time after long break")
                    self.backoff_minutes = 0
            
            self.last_restart_time = current_time
            return True
            
        except Exception as e:
            logger.error(f"Failed to start script: {e}")
            return False

    def _monitor_output(self, pipe, name):
        """Monitor and forward output from the script."""
        try:
            for line in pipe:
                clean_line = line.rstrip()
                if name == "STDOUT":
                    logger.info(f"[Script] {clean_line}")
                else:
                    logger.error(f"[Script-Error] {clean_line}")
        except Exception as e:
            logger.error(f"Error monitoring {name}: {e}")

    def check_script_status(self):
        """Check if the script is still running."""
        if not self.process:
            return False
            
        # Check if process is still running
        if self.process.poll() is not None:
            exit_code = self.process.poll()
            
            # Calculate how long the script ran
            run_time = datetime.datetime.now() - self.last_start_time
            run_seconds = run_time.total_seconds()
            self.total_runtime += run_seconds
            
            # Update longest runtime stat
            if run_seconds > self.longest_runtime:
                self.longest_runtime = run_seconds
            
            # Log the termination with runtime information
            if exit_code == 0:
                logger.warning(f"Script completed normally (exit code 0) after running for {run_seconds:.1f} seconds")
                
                # Count it as a successful run
                self.successful_runs += 1
                
                # Reset restart count if script ran for a long time (over 30 minutes)
                if run_seconds > 1800:  # 30 minutes
                    old_count = self.restart_count
                    self.restart_count = 0
                    logger.info(f"Reset restart counter from {old_count} to 0 after a long successful run")
                    self.backoff_minutes = 0
            else:
                logger.warning(f"Script crashed with exit code: {exit_code} after running for {run_seconds:.1f} seconds")
            
            # Record when disconnection happened if not already set
            if not self.disconnection_time:
                self.disconnection_time = datetime.datetime.now()
                
            return False
            
        return True

    def restart_script(self):
        """Try to restart the script."""
        # Check internet connection
        internet_available = self.check_internet_connection()
        logger.info(f"Internet connection available: {internet_available}")
        
        # Apply adaptive retry with backoff if we're having too many quick failures
        current_delay = self.restart_delay
        if self.restart_count > 10 and self.longest_runtime < 60:
            # If we've had many quick failures, increase backoff
            self.backoff_minutes = min(30, self.backoff_minutes + 1)
            current_delay = self.backoff_minutes * 60  # Convert to seconds
            logger.warning(f"Too many quick failures. Using backoff delay of {self.backoff_minutes} minutes")
        
        # Wait before restarting
        logger.info(f"Waiting {current_delay} seconds before restart attempt...")
        time.sleep(current_delay)
        
        # Increment restart counters
        self.restart_count += 1
        self.total_restarts += 1
        
        # Add restart counter info
        logger.info(f"Attempting restart #{self.restart_count} (total: {self.total_restarts}, successful runs: {self.successful_runs})...")
        
        # Terminate existing process if it's somehow still running
        self.terminate_script()
        
        # Start script again
        return self.start_script()

    def terminate_script(self):
        """Terminate the script if it's running."""
        if self.process and self.process.poll() is None:
            try:
                logger.info(f"Terminating process {self.process.pid}...")
                if platform.system() == "Windows":
                    # On Windows, use taskkill to ensure termination
                    subprocess.call(['taskkill', '/F', '/T', '/PID', str(self.process.pid)])
                else:
                    # On Unix, use SIGTERM then SIGKILL
                    self.process.terminate()
                    time.sleep(3)
                    if self.process.poll() is None:
                        self.process.kill()
                        
                logger.info("Process terminated.")
            except Exception as e:
                logger.error(f"Error terminating process: {e}")
                
    def check_alert_threshold(self):
        """Check if an alert should be triggered based on disconnection time."""
        if not self.disconnection_time or self.alert_triggered:
            return
            
        # Calculate how long the script has been disconnected
        disconnection_duration = datetime.datetime.now() - self.disconnection_time
        disconnection_minutes = disconnection_duration.total_seconds() / 60
        
        if disconnection_minutes >= self.alert_threshold_minutes:
            logger.warning(f"ALERT: Script has been disconnected for {disconnection_minutes:.1f} minutes!")
            self.play_alert_sound()
            self.alert_triggered = True

    def run(self):
        """Main monitoring loop."""
        logger.info(f"Starting Script Keeper for: {self.script_path}")
        logger.info(f"Arguments: {' '.join(self.script_args)}")
        logger.info(f"Settings: Alert after {self.alert_threshold_minutes} min, Check every {self.check_interval} sec")
        logger.info(f"UNLIMITED RESTARTS ENABLED - Will keep trying forever")
        
        self.start_script()
        
        try:
            while True:
                # Check if script is running
                script_running = self.check_script_status()
                
                if not script_running:
                    # Script is not running, try to restart it
                    logger.warning("Script is not running. Attempting restart.")
                    restart_success = self.restart_script()
                    
                    if not restart_success:
                        # Check if we need to trigger an alert
                        self.check_alert_threshold()
                
                # Wait before next check
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            logger.info("Keeper interrupted by user (Ctrl+C).")
            self.terminate_script()
        except Exception as e:
            logger.error(f"Error in monitoring loop: {e}")
            self.terminate_script()

def main():
    """Parse arguments and start the script monitor."""
    if len(sys.argv) < 2:
        print("\n==== Script Keeper ====")
        print(f"Usage: {sys.argv[0]} <your_script.py> [script arguments]")
        print("\nExample: python script_keeper.py main.py arg1 arg2")
        print("This will keep main.py running continuously.\n")
        sys.exit(1)
        
    script_path = sys.argv[1]
    script_args = sys.argv[2:]
    
    # Check if script exists
    if not os.path.exists(script_path):
        print(f"Error: Script not found: {script_path}")
        sys.exit(1)
        
    # Get configuration from environment variables or use defaults
    restart_delay = int(os.environ.get("KEEPER_RESTART_DELAY", "5"))
    alert_threshold_minutes = int(os.environ.get("KEEPER_ALERT_THRESHOLD", "30"))
    check_interval = int(os.environ.get("KEEPER_CHECK_INTERVAL", "5"))
    
    # Create and run the keeper
    keeper = ScriptKeeper(
        script_path=script_path,
        script_args=script_args,
        restart_delay=restart_delay,
        alert_threshold_minutes=alert_threshold_minutes,
        check_interval=check_interval
    )
    
    keeper.run()

if __name__ == "__main__":
    main()