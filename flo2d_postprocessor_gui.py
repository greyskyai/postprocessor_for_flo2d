import tkinter as tk
from tkinter import ttk, filedialog, messagebox
import sys
import io
import os
from main import batch_process_flo2d, process_flo2d
import shutil
import threading
import json
from ttkthemes import ThemedStyle

CONFIG_FILE = "config.json"

class RedirectText(io.StringIO):
    def __init__(self, text_widget):
        super().__init__()
        self.text_widget = text_widget

    def write(self, string):
        self.text_widget.configure(state='normal')
        self.text_widget.insert(tk.END, string)
        self.text_widget.see(tk.END)
        self.text_widget.configure(state='disabled')

class ToolTip:
    """Tooltip for widgets"""
    def __init__(self, widget, text):
        self.widget = widget
        self.text = text
        self.tooltip_window = None
        widget.bind("<Enter>", self.show_tooltip)
        widget.bind("<Leave>", self.hide_tooltip)

    def show_tooltip(self, event=None):
        if self.tooltip_window or not self.text:
            return
        # Determine tooltip position based on widget type
        if isinstance(self.widget, (tk.Entry, tk.Text)):
            try:
                x, y, cx, cy = self.widget.bbox("insert")
                x += self.widget.winfo_rootx() + 25
                y += self.widget.winfo_rooty() + 20
            except tk.TclError:
                # Fallback if "insert" is not available
                x = self.widget.winfo_rootx() + 25
                y = self.widget.winfo_rooty() + 20
        else:
            # For other widgets, position tooltip near the widget
            x = self.widget.winfo_rootx() + 25
            y = self.widget.winfo_rooty() + 20
        self.tooltip_window = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{x}+{y}")
        label = tk.Label(tw, text=self.text, background="#3E3E3E", foreground="#FFFFFF",
                         relief='solid', borderwidth=1, wraplength=200)
        label.pack(ipadx=1)

    def hide_tooltip(self, event=None):
        tw = self.tooltip_window
        self.tooltip_window = None
        if tw:
            tw.destroy()

class FLO2DPostProcessorGUI:
    def __init__(self, master):
        self.master = master
        self.master.title("FLO2D Post-Processor Lite")
        self.master.geometry("800x800")  # Increased height to accommodate new widgets
        self.master.configure(bg="#2E2E2E")

        # Initialize ThemedStyle and set to 'equilux'
        self.style = ThemedStyle(self.master)
        self.style.set_theme("equilux")  # Set 'equilux' as the exclusive theme

        self.create_widgets()
        self.load_settings()

        # Bind the close event to save settings
        self.master.protocol("WM_DELETE_WINDOW", self.on_close)

    def create_widgets(self):
        main_frame = ttk.Frame(self.master, padding="15")
        main_frame.grid(row=0, column=0, sticky=(tk.W, tk.E, tk.N, tk.S))
        self.master.columnconfigure(0, weight=1)
        self.master.rowconfigure(0, weight=1)

        # FLO-2D Folders Section
        folders_label = ttk.Label(main_frame, text="FLO-2D Folders:")
        folders_label.grid(column=0, row=0, sticky=tk.W)
        ToolTip(folders_label, "Select one or more FLO-2D project folders to process.")

        # Customize Listbox with black background and white text
        self.folder_listbox = tk.Listbox(main_frame, width=50, height=5, bg="#000000", fg="#FFFFFF", selectmode=tk.MULTIPLE)
        self.folder_listbox.grid(column=0, row=1, sticky=(tk.W, tk.E))
        ToolTip(self.folder_listbox, "List of FLO-2D project folders to be processed.")

        folder_buttons_frame = ttk.Frame(main_frame)
        folder_buttons_frame.grid(column=1, row=1, sticky=tk.NW, padx=(10,0))
        add_btn = ttk.Button(folder_buttons_frame, text="Add", command=self.add_folder)
        add_btn.grid(column=0, row=0, sticky=tk.W, pady=(0, 5))
        ToolTip(add_btn, "Add a FLO-2D project folder to the list.")
        remove_btn = ttk.Button(folder_buttons_frame, text="Remove", command=self.remove_folder)
        remove_btn.grid(column=0, row=1, sticky=tk.W)
        ToolTip(remove_btn, "Remove the selected folder(s) from the list.")

        # EPSG Number Section
        epsg_label = ttk.Label(main_frame, text="EPSG Number:")
        epsg_label.grid(column=0, row=2, sticky=tk.W, pady=(15, 0))
        ToolTip(epsg_label, "Enter the EPSG code for spatial reference.")

        self.epsg_number = ttk.Entry(main_frame, width=20)
        self.epsg_number.grid(column=0, row=3, sticky=tk.W)
        ToolTip(self.epsg_number, "Enter a valid integer EPSG code (e.g., 4326).")

        # Shapefile Option
        self.create_shapefile = tk.BooleanVar()
        shapefile_cb = ttk.Checkbutton(
            main_frame,
            text="Create FLO-2D Data Points Shapefile",
            variable=self.create_shapefile,
            style='TCheckbutton'
        )
        shapefile_cb.grid(column=0, row=4, sticky=tk.W, pady=(10, 0))
        ToolTip(shapefile_cb, "Check to generate a shapefile of FLO-2D data points.")

        # --- New Section: Output Format Selection ---
        output_format_label = ttk.Label(main_frame, text="Output Format:")
        output_format_label.grid(column=0, row=5, sticky=tk.W, pady=(15, 0))
        ToolTip(output_format_label, "Select the format for saving the output data.")

        self.output_format = tk.StringVar(value="Shapefile")  # Default selection

        # Radio Buttons for Output Format
        shapefile_rb = ttk.Radiobutton(
            main_frame,
            text="Shapefile",
            variable=self.output_format,
            value="Shapefile"
        )
        shapefile_rb.grid(column=0, row=6, sticky=tk.W)
        ToolTip(shapefile_rb, "Save output data as Shapefile.")

        geopackage_rb = ttk.Radiobutton(
            main_frame,
            text="GeoPackage",
            variable=self.output_format,
            value="GeoPackage"
        )
        geopackage_rb.grid(column=0, row=7, sticky=tk.W)
        ToolTip(geopackage_rb, "Save output data as GeoPackage.")

        # Style Files Folder Section
        style_label = ttk.Label(main_frame, text="Style Files Folder:")
        style_label.grid(column=0, row=8, sticky=tk.W, pady=(15, 0))
        ToolTip(style_label, "Select the folder containing style files for processing.")

        style_frame = ttk.Frame(main_frame)
        style_frame.grid(column=0, row=9, sticky=(tk.W, tk.E))
        self.style_folder = ttk.Entry(style_frame, width=40, state='readonly')  # Set to readonly to prevent manual editing
        self.style_folder.grid(column=0, row=0, sticky=(tk.W, tk.E))
        ToolTip(self.style_folder, "Path to the folder containing style files.")
        browse_style_btn = ttk.Button(style_frame, text="Browse", command=self.browse_style_folder)
        browse_style_btn.grid(column=1, row=0, sticky=tk.W, padx=(5,0))
        ToolTip(browse_style_btn, "Browse to select the style files folder.")

        style_frame.columnconfigure(0, weight=1)

        # Output Section
        output_label = ttk.Label(main_frame, text="Output:")
        output_label.grid(column=0, row=10, sticky=tk.W, pady=(15, 0))
        ToolTip(output_label, "Displays the processing logs and results.")

        # Customize Text widget with black background and white text
        self.output_text = tk.Text(main_frame, wrap=tk.WORD, width=80, height=15, bg="#000000", fg="#FFFFFF", state='disabled')
        self.output_text.grid(column=0, row=11, columnspan=2, sticky=(tk.W, tk.E, tk.N, tk.S))
        ToolTip(self.output_text, "Log output of the processing steps.")

        # Progress Bar
        self.progress = ttk.Progressbar(main_frame, mode='indeterminate')
        self.progress.grid(column=0, row=12, columnspan=2, sticky=(tk.W, tk.E), pady=(10,0))

        # Run Button
        run_btn = ttk.Button(main_frame, text="Run", command=self.run_process_thread)
        run_btn.grid(column=1, row=13, sticky=tk.E, pady=(10,0))
        ToolTip(run_btn, "Start processing the selected FLO-2D folders.")

        # Configure grid weights for responsiveness
        main_frame.columnconfigure(0, weight=1)
        main_frame.columnconfigure(1, weight=0)
        main_frame.rowconfigure(11, weight=1)

    def add_folder(self):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            if folder_selected not in self.folder_listbox.get(0, tk.END):
                self.folder_listbox.insert(tk.END, folder_selected)
                self.save_settings()
            else:
                messagebox.showinfo("Duplicate Folder", "The selected folder is already in the list.")

    def remove_folder(self):
        selected_indices = self.folder_listbox.curselection()
        if not selected_indices:
            messagebox.showwarning("No Selection", "Please select at least one folder to remove.")
            return
        for index in reversed(selected_indices):
            self.folder_listbox.delete(index)
        self.save_settings()

    def browse_style_folder(self):
        folder_selected = filedialog.askdirectory()
        if folder_selected:
            self.style_folder.configure(state='normal')
            self.style_folder.delete(0, tk.END)
            self.style_folder.insert(0, folder_selected)
            self.style_folder.configure(state='readonly')
            self.save_settings()

    def run_process_thread(self):
        # Validate inputs before starting
        if not self.folder_listbox.size():
            messagebox.showerror("No Folders", "Please add at least one FLO-2D folder to process.")
            return
        epsg = self.epsg_number.get()
        if not epsg.isdigit():
            messagebox.showerror("Invalid EPSG", "Please enter a valid integer for the EPSG number.")
            return
        style_folder = self.style_folder.get()
        if style_folder and not os.path.isdir(style_folder):
            messagebox.showerror("Invalid Style Folder", "The specified style files folder does not exist.")
            return

        # Disable run button and input widgets that support 'state'
        self.set_widgets_state(main_frame=self.master, state='disabled')

        # Start progress bar
        self.progress.start()

        # Run processing in a separate thread to keep GUI responsive
        threading.Thread(target=self.run_process, daemon=True).start()

    def run_process(self):
        self.output_text.configure(state='normal')
        self.output_text.delete('1.0', tk.END)
        self.output_text.configure(state='disabled')

        old_stdout = sys.stdout
        sys.stdout = RedirectText(self.output_text)

        try:
            file_paths = list(self.folder_listbox.get(0, tk.END))
            for file_path in file_paths:
                print(f"Processing: {file_path}")
                print("-" * 50)
                result = process_flo2d(
                    file_path,
                    int(self.epsg_number.get()),
                    self.create_shapefile.get(),
                    verbose=True,
                    style_folder=self.style_folder.get(),
                    output_format=self.output_format.get()  # Pass output format
                )
                print(result)
                print("\n")
            
            # Final completion message
            completion_message = "\n" + "=" * 50 + "\n"
            completion_message += "All FLO-2D folders processed successfully\n"
            completion_message += "=" * 50 + "\n"
            print(completion_message)
        except Exception as e:
            print(f"An error occurred: {str(e)}")
            messagebox.showerror("Processing Error", f"An error occurred during processing:\n{str(e)}")
        finally:
            sys.stdout = old_stdout
            self.progress.stop()
            self.set_widgets_state(main_frame=self.master, state='normal')

    def set_widgets_state(self, main_frame, state):
        """Recursively set the state of widgets that support the 'state' option."""
        for child in main_frame.winfo_children():
            # Skip frames
            if isinstance(child, ttk.Frame):
                self.set_widgets_state(child, state)
                continue
            # Check if widget has 'state' option
            try:
                child.configure(state=state)
            except tk.TclError:
                # Widget does not support 'state' option
                pass

    def load_settings(self):
        """Load settings from the configuration file."""
        if not os.path.exists(CONFIG_FILE):
            return  # No settings to load

        try:
            with open(CONFIG_FILE, 'r') as f:
                config = json.load(f)
            
            # Load FLO-2D Folders
            folders = config.get("flo2d_folders", [])
            for folder in folders:
                if os.path.isdir(folder):
                    self.folder_listbox.insert(tk.END, folder)
            
            # Load EPSG Number
            epsg = config.get("epsg_number", "")
            self.epsg_number.insert(0, epsg)
            
            # Load Shapefile Option
            shapefile = config.get("create_flo2d_points", False)
            self.create_shapefile.set(shapefile)
            
            # Load Style Files Folder
            style_folder = config.get("style_folder", "")
            self.style_folder.configure(state='normal')
            self.style_folder.insert(0, style_folder)
            self.style_folder.configure(state='readonly')
            
            # Load Output Format
            output_format = config.get("output_format", "Shapefile")
            self.output_format.set(output_format)
            
        except Exception as e:
            messagebox.showwarning("Load Settings", f"Failed to load settings:\n{str(e)}")

    def save_settings(self):
        """Save current settings to the configuration file."""
        config = {
            "flo2d_folders": list(self.folder_listbox.get(0, tk.END)),
            "epsg_number": self.epsg_number.get(),
            "create_flo2d_points": self.create_shapefile.get(),
            "style_folder": self.style_folder.get(),
            "output_format": self.output_format.get()  # Save output format
        }
        try:
            with open(CONFIG_FILE, 'w') as f:
                json.dump(config, f, indent=4)
        except Exception as e:
            messagebox.showwarning("Save Settings", f"Failed to save settings:\n{str(e)}")

    def on_close(self):
        """Handle the window close event."""
        self.save_settings()
        self.master.destroy()

def main():
    root = tk.Tk()
    app = FLO2DPostProcessorGUI(root)
    root.mainloop()

if __name__ == "__main__":
    main()
