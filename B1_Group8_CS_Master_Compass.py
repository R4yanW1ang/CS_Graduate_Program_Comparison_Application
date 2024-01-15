"""
@File name: cs_master_compass
@Andrew IDS: yangyond, hhe3, mfouad, ziruiw2
@Purpose: Create a user-friendly GUI interface to compare graduate school programs.
"""

import os
import shutil
import time
import tkinter as tk
from tkinter import ttk
import pandas as pd
import matplotlib.pyplot as plt
from map_plot import map_plot
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import Pmw
from data_collection import (collect_and_clean_pop, collect_and_clean_safety,
                             collect_and_clean_weather, collect_and_clean_program)
from data_cleaning_merge import merge
from create_scatterplot import prep_weather_data_for_scatterplot


class CompassApp:
    def __init__(self, root):
        """ Create a user-friendly GUI interface to compare graduate school programs.
        This app can:
        (1) display information of programs by filtering on multiple criteria；
        (2) draw diagram for visualization.
        """
        self.combobox_vars = {}
        self.root = root
        self.root.title("CS Master Compass")
        self.tip = Pmw.Balloon(self.root)
        self.canvas1 = None
        self.canvas2 = None
        self.canvas3 = None

        # Load data
        self.labels = ["Ranking", "State", "City", "University", "Points", "population_category",
                       "temperature_category", "safety_category"]
        self.columns_display = [["University", "Ranking", "State", "City", "Points"],
                                ["In_State_Tuition", "Out_of_State_Tuition"],
                                ["population_category", "Population_estimate_2022"],
                                ["safety_category", "Total_criminal_count"],
                                ["temperature_category", "Temperature_avg_spring", "Temperature_avg_summer",
                                 "Temperature_avg_fall", "Temperature_avg_winter"],
                                ["Description"]]
        self.collect_funcs = [collect_and_clean_pop, collect_and_clean_safety,
                              collect_and_clean_weather, collect_and_clean_program,
                              merge,
                              prep_weather_data_for_scatterplot]
        self.df = None
        self.df_weather = None
        self.create_first_canvas()

    def create_first_canvas(self):
        """
        Create the first canvas to guide user to choose weather to scrape fresh data or use scraped data
        """
        self.canvas1 = tk.Frame(self.root)
        self.canvas1.pack(fill="both", expand=True)

        self.label1 = tk.Label(self.canvas1,
                               text="Welcome! Please use this Graduate School Comparison Tool to pick a cs master's program that works for you.",
                               anchor='w', justify='left')
        self.label1.pack(pady=20, fill='x')

        self.label2 = tk.Label(self.canvas1,
                               text="We scrape websites to collect important information for you to compare master's program. Since scraping may take up to an hour, you can choose to scrape fresh data or use previously scraped data directly.",
                               anchor='w', justify='left')
        self.label2.pack(pady=20, fill='x')

        self.choice_var = tk.StringVar(value="")

        self.btn_scrape = tk.Button(self.canvas1, text="Scrape Fresh Data", command=lambda: self.set_choice("scrape"))
        self.btn_scrape.pack(side="left", padx=10)

        self.progress_scrape = ttk.Progressbar(self.canvas1, orient="horizontal", length=150, mode="determinate")
        self.progress_scrape.pack(side="left", padx=10)

        self.progress_label_scrape = tk.Label(self.canvas1, text="0%", anchor='w')
        self.progress_label_scrape.pack(side="left", padx=10)

        self.btn_use = tk.Button(self.canvas1, text="Use Scraped Data", command=lambda: self.set_choice("use"))
        self.btn_use.pack(side="right", padx=10)

        self.progress_use = ttk.Progressbar(self.canvas1, orient="horizontal", length=150, mode="determinate")
        self.progress_use.pack(side="right", padx=10)

        self.progress_label_use = tk.Label(self.canvas1, text="0%", anchor='w')
        self.progress_label_use.pack(side="right", padx=10)

        self.btn_continue = tk.Button(self.canvas1, text="CONTINUE", command=self.goto_canvas2, state=tk.DISABLED)
        self.btn_continue.pack(pady=20, anchor='center')

    def create_second_canvas(self):
        """
        Create the second canvas to filter programs and plot diagrams.
        """
        self.canvas2 = tk.Frame(self.root)

        # Create drop-down boxes
        self.create_comboboxes()

        # Create a button so that user can click to view the data
        self.view_button = ttk.Button(self.canvas2, text="Filter", command=self.view_data)
        self.view_button.grid(row=4, column=3, padx=5, pady=5)

        # Create text box to show the information about university
        self.text = tk.Text(self.canvas2, wrap=tk.WORD, width=100, height=20)
        self.text.grid(row=5, column=0, columnspan=30, padx=5, pady=5)

        # Create a plot button to draw diagram
        self.plot_button = ttk.Button(self.canvas2, text="Plot", command=self.plot_data)
        self.plot_button.grid(row=4, column=6, padx=5, pady=5)

        # Create a reset button to clear the selected values of all filters
        self.reset_button = ttk.Button(self.canvas2, text="Reset", command=self.reset)
        self.reset_button.grid(row=4, column=4, padx=5, pady=5)

        # Create a error message text
        self.error_message = ttk.Label(self.canvas2, text="", foreground="red")
        self.error_message.grid(row=4, column=9, columnspan=2, sticky=(tk.W, tk.E))

        # # Add a hint to the view and plot button
        self.tip.bind(self.view_button, "Select criteria values, press filter and display information of university.")
        self.tip.bind(self.plot_button, "Scatter: select a university to plot the monthly temperature scatter chart.\n"
                                        "Map: directly click the Plot button to draw a map to display university counts in each state.")

       # Add a back button to return to the first canvas
       #self.btn_back = tk.Button(self.canvas2, text="Back", command=self.goto_canvas1)
       #self.btn_back.grid(row=4, column=0, padx=5, pady=5)

    def set_choice(self, choice):
        """
        Scrape websites or directly load scraped data based on user's choice.
        :param choice: scrape fresh data or use scraped data
        """
        self.choice_var.set(choice)

        if choice == "scrape":
            progress_bar = self.progress_scrape
            progress_label = self.progress_label_scrape

            # Remove the previous data folders and data files.
            self.clear_scraped_data()

            # Scrape websites, clean data and merge data. Display progress bar in UI interface.
            data_list = []
            for i, func in enumerate(self.collect_funcs):
                # Scrape data from four sources
                if i < 4:
                    data_list.append(func())
                # Clean and merge those scraped data
                elif i == 4:
                    self.df = func(*data_list)
                else:
                    # Load monthly temperature data to draw scatterplot
                    self.df_weather = func()
                # Display progress bar
                progress_value = 100 if i == 5 else (i + 1) * 16
                progress_bar['value'] = progress_value
                progress_label['text'] = f"{progress_value}%"
                self.root.update_idletasks()

        else:
            progress_bar = self.progress_use
            progress_label = self.progress_label_use

            # Load scraped data directly
            data_list = []
            for i, data in enumerate(self.load_data()):
                time.sleep(0.5)
                data_list.append(data)
                progress_value = (i + 1) * 50
                progress_bar['value'] = progress_value
                progress_label['text'] = f"{progress_value}%"
                self.root.update_idletasks()
            self.df, self.df_weather = data_list
            self.df['Ranking'] = self.df.apply(lambda x: str(x['Ranking']), axis=1)

        # 加载完毕后，允许用户点击CONTINUE按钮
        self.btn_continue.config(state=tk.NORMAL)

    def goto_canvas1(self):
        self.canvas2.pack_forget()
        self.canvas1.pack(fill="both", expand=True)

    def goto_canvas2(self):
        # After user click the CONTINUE button, create the second canvas
        self.create_second_canvas()
        self.canvas1.pack_forget()
        self.canvas2.pack(fill="both", expand=True)

    @staticmethod
    def clear_scraped_data():
        """
        Remove data folders and clear data files.
        """
        for directory in ["download", "population", "safety", "weather", "program", "merge"]:
            if os.path.exists(directory):
                shutil.rmtree(directory)
                print(f"{directory} has been removed!")
            else:
                print(f"{directory} does not exist!")

    @staticmethod
    def load_data():
        """Load data such as school ranking, safety, climate,...,etc.
        """
        # Load data for filter
        yield pd.read_csv(os.path.join("merge_previous", "merged.csv"))
        # Load data to plot chart
        yield pd.read_csv(os.path.join("merge_previous", "weather_merged.csv"))


    def create_comboboxes(self):
        """Create drop-down boxes for user to filter program.
        """
        for idx, label in enumerate(self.labels):
            ttk.Label(self.canvas2, text=label, anchor='w').grid(row=idx // 4, column=(idx % 4) * 3, sticky='w')
            self.combobox_vars[label] = tk.StringVar()
            combobox = ttk.Combobox(self.canvas2, textvariable=self.combobox_vars[label])
            if label == "Ranking":
                values = ["All"] + ["Top%d" % i for i in
                                    [j for j in range(10, ((len(self.df) - 1) // 10 + 1) * 10 + 1, 10)]]
            elif label == "Points":
                values = ["All"] + [f"{i}-{i + 10 - 1}" for i in range(1, self.df["Points"].max(), 10)]
            else:
                values = ["All"] + self.df[label].unique().tolist()
            combobox['values'] = values
            combobox.set("All")
            combobox.grid(row=idx // 4, column=(idx % 4) * 3 + 1, padx=3, pady=3)

        # Create another combobox to choose which diagram to plot
        self.combobox_vars["Plot"] = tk.StringVar()
        combobox = ttk.Combobox(self.canvas2, textvariable=self.combobox_vars["Plot"])
        values = ["All", "Scatter", "Map"]
        combobox['values'] = values
        combobox.set("All")
        combobox.grid(row=4, column=7, padx=5, pady=5)

    def view_data(self):
        """ Filter program based on the selection in the drop-down box
        """
        mask = [True] * len(self.df)
        for label, var in self.combobox_vars.items():
            if label == "Plot": continue

            value = var.get()
            if value:
                if value == "All":
                    continue
                if label == "Ranking":
                    value = int(value.strip("Top"))
                    mask &= (self.df[label].isin([str(i + 1) for i in range(int(value))]))
                elif label == "Points":
                    lower_bound = int(value.split("-")[0])
                    upper_bound = int(value.split("-")[1])
                    mask &= (self.df[label].isin([i for i in range(lower_bound, upper_bound + 1)]))
                else:
                    mask &= (self.df[label] == value)
        result = self.df[mask]

        # Update the content in text box
        self.update_display(result)

    def update_display(self, data):
        # Clear the current data
        self.text.delete(1.0, tk.END)
        # Add data
        for i in range(len(data)):
            df_row = data.iloc[[i], :]

            for j, columns in enumerate(self.columns_display):
                if j == 0 or j == 5:
                    for _, row in df_row[columns].iterrows():
                        for col, value in row.items():
                            self.text.insert(tk.END, f"{col}: {value}\n")
                elif j == 1:
                    in_state_tuition, out_state_tuition = df_row[columns].values.tolist()[0]

                    self.text.insert(tk.END, f"Tuition: "
                                             f"in state tuition is ${int(in_state_tuition)}, "
                                             f"out state tuition is ${int(out_state_tuition)}\n")
                elif j == 2:
                    pop_category, pop_2022 = df_row[columns].values.tolist()[0]
                    self.text.insert(tk.END, f"Population: "
                                             f"relatively {pop_category} size, "
                                             f"number estimated in 2022 is {int(pop_2022)}\n")
                elif j == 3:
                    safety_category, total_criminal_count = df_row[columns].values.tolist()[0]
                    self.text.insert(tk.END, f"Safety: "
                                             f"relatively {safety_category} risk of danger, "
                                             f"total criminal count in 2021 is {int(total_criminal_count)}\n")
                elif j == 4:
                    temperates = df_row[columns].values.tolist()[0]
                    temperates = [round(v, 2) if i >= 1 else v for i, v in enumerate(temperates)]
                    self.text.insert(tk.END, "Temperature: {}, "
                                             "average value in 2022: spring is {}°C, summer is {}°C, "
                                             "fall is {}°C, winter is {}°C\n".format(*temperates))

            self.text.insert(tk.END, "\n")

    def reset(self):
        """
        Clear the selected values of all filters
        """
        for label in self.labels:
            self.combobox_vars[label].set("All")

    def plot_data(self):
        """ Plot scatter chart or map base on user's choice
        """
        plot_type = self.combobox_vars["Plot"].get()
        if plot_type == "Scatter":
            self.plot_scatter()
        elif plot_type == "Map":
            self.plot_map()
        else:
            self.plot_scatter()
            self.plot_map()

    def plot_scatter(self):
        university = self.combobox_vars['University'].get()
        if university == "All":
            # Show the error message
            self.error_message.config(text="Please select an University")
        elif university and university != "All":
            # Delete the error message
            self.error_message.config(text="")
            # Filter the data to the university of choice
            weather_processed_df = self.df_weather[self.df_weather["University"] == university]
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']

            fig = plt.figure(figsize=(7, 4))
            plt.scatter(x=months, y=weather_processed_df['Temperature_avg'])
            title = "Average Temperature in " + university
            plt.title(title)
            plt.xlabel("Month")
            plt.ylabel("Avg Temperature (C)")

            self.canvas3 = FigureCanvasTkAgg(fig, master=self.canvas2)
            canvas_widget = self.canvas3.get_tk_widget()
            canvas_widget.grid(row=6, column=0, columnspan=30, padx=10, pady=10)
            self.canvas3.draw()

    def plot_map(self):
        map_plot(self.df)


root = tk.Tk()
app = CompassApp(root)
root.mainloop()