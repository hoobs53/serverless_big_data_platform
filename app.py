import tkinter as tk
from tkinter import filedialog
from aws import init as init_aws, invoke_coordinator
import json


def save_to_file(result):
    print("Saving results to file...")
    with open("results.json", "w") as f:
        f.write(json.dumps(result))


class App:
    def __init__(self, master):
        self.master = master
        master.geometry("800x450")
        master.title("AWS Big Data Platform")

        # Create the submit file and text buttons, and place them in the middle
        self.init_frame = tk.Frame(master)
        self.init_frame.pack(side=tk.TOP, fill=tk.X, pady=(20, 0))

        # Create the update lambda checkbox, and place it in the middle
        self.update_lambda_var = tk.BooleanVar()
        self.update_lambda_var.set(False)

        self.update_lambda_checkbox = tk.Checkbutton(self.init_frame, text="Update Lambda",
                                                     variable=self.update_lambda_var)
        self.update_lambda_checkbox.pack(side=tk.LEFT, expand=True)

        # Create the init button, and place it in the middle
        self.init_button = tk.Button(self.init_frame, text="Init", command=self.init_callback)
        self.init_button.pack(side=tk.RIGHT, expand=True)

        self.frame = tk.Frame(master)
        self.frame.pack(side=tk.TOP, fill=tk.BOTH, expand=True)

        self.text_label = tk.Label(self.frame, text="Text Input:")
        self.text_label.pack(side=tk.LEFT, padx=(20, 10), pady=(20, 0))

        self.text_entry = tk.Text(self.frame, width=50, height=10)
        self.text_entry.pack(side=tk.LEFT, padx=(0, 20), pady=(20, 0))

        # Create the file input label, widget, and browse button, and place them on the right
        self.browse_button = tk.Button(self.frame, text="Browse", command=self.browse_file)
        self.browse_button.pack(side=tk.RIGHT, padx=(10, 20), pady=(20, 0))

        self.file_entry = tk.Entry(self.frame)
        self.file_entry.pack(side=tk.RIGHT, padx=(0, 10), pady=(20, 0))

        self.file_label = tk.Label(self.frame, text="File Input:")
        self.file_label.pack(side=tk.RIGHT, pady=(20, 0))

        # Create the submit file and text buttons, and place them in the middle
        self.submit_frame = tk.Frame(master)
        self.submit_frame.pack(side=tk.TOP, fill=tk.X, pady=(20, 0))

        self.file_submit_button = tk.Button(self.submit_frame, text="Submit File", command=self.submit_file)
        self.file_submit_button.pack(side=tk.RIGHT, padx=(0, 120))

        self.text_submit_button = tk.Button(self.submit_frame, text="Submit Text", command=self.submit_text)
        self.text_submit_button.pack(side=tk.LEFT, padx=(230, 0))

        # Create the result label, and place it at the bottom
        self.result_label = tk.Label(master, text="")
        self.result_label.pack(side=tk.BOTTOM, pady=(50, 20))

    def submit_text(self):
        contents = self.text_entry.get("1.0", "end-1c")
        self.send_request(contents)

    def browse_file(self):
        file_path = filedialog.askopenfilename()
        self.file_entry.delete(0, tk.END)
        self.file_entry.insert(0, file_path)

    def submit_file(self):
        file_path = self.file_entry.get()
        with open(file_path, 'r') as f:
            contents = f.read()
        self.send_request(contents)

    def init_callback(self):
        self.file_submit_button["state"] = "disabled"
        self.text_submit_button["state"] = "disabled"
        update_lambdas = self.update_lambda_var.get()
        init_aws(update_lambdas)
        self.file_submit_button["state"] = "normal"
        self.text_submit_button["state"] = "normal"

    def send_request(self, contents):
        self.result_label.configure(text="Sending request to aws...")
        result, time_elapsed = invoke_coordinator(contents)
        self.result_label.configure(text="Processing ended in: " + str(round(time_elapsed, 3)) + " s\n Result: " + str(
            result["data"]) + "\nLogs: " + str(result["stats"]))
        save_to_file(result)


if __name__ == "__main__":
    root = tk.Tk()
    my_app = App(root)
    root.mainloop()
