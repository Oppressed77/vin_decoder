import sys
import requests
import pandas as pd
from PyQt5.QtWidgets import QComboBox, QApplication, QWidget, QFileDialog, QLabel, QPushButton, QProgressBar, QTextEdit, \
    QVBoxLayout, QHBoxLayout

class VINDecoder(QWidget):
    def __init__(self):
        super().__init__()
        # Set up the GUI
        self.setWindowTitle('VIN Decoder')
        self.setGeometry(100, 100, 800, 600)
        self.layout = QVBoxLayout()
        # Add a label for the file name
        self.file_label = QLabel('No file selected')
        self.layout.addWidget(self.file_label)
        # Add a button for selecting a file
        self.file_button = QPushButton('Select File')
        self.file_button.clicked.connect(self.select_file)
        self.layout.addWidget(self.file_button)
        # Add a label for the VIN column selection
        self.vin_label = QLabel('Select VIN Column:')
        self.layout.addWidget(self.vin_label)
        # Add a combo box for selecting the VIN column
        self.vin_column = QComboBox()
        self.layout.addWidget(self.vin_column)
        # Add a text edit for displaying the input file
        self.input_text = QTextEdit()
        self.input_text.setReadOnly(True)
        self.layout.addWidget(self.input_text)
        # Add a progress bar
        self.progress_bar = QProgressBar()
        self.progress_bar.setRange(0, 100)
        self.progress_bar.setValue(0)
        self.layout.addWidget(self.progress_bar)
        # Add a button for decoding the VINs
        self.decode_button = QPushButton('Decode')
        self.decode_button.clicked.connect(self.decode_vins)
        self.decode_button.setDisabled(True)
        self.layout.addWidget(self.decode_button)
        # Add the layout to the window
        self.setLayout(self.layout)
        self.show()
    def select_file(self):
        # Show a file dialog to select an Excel or CSV file
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        file_name, _ = QFileDialog.getOpenFileName(self, 'Select File', '', 'Excel Files (*.xlsx);;CSV Files (*.csv)', options=options)
        # Update the file label and load the file into a Pandas DataFrame
        if file_name:
            self.file_label.setText(file_name)
            self.df = pd.read_excel(file_name) if file_name.endswith('.xlsx') else pd.read_csv(file_name)
            self.input_text.setPlainText(str(self.df.head()))
            self.vin_column.addItems(self.df.columns)
            self.decode_button.setEnabled(True)

    def decode_vins(self):
        # Get the VIN column from the DataFrame
        vin_col = self.df[self.vin_column.currentText()]
        # Split the VINs into batches of 50 and send to the NHTSA API
        batches = [vin_col[i:i + 50] for i in range(0, len(vin_col), 50)]
        decoded_data = []
        for i, batch in enumerate(batches):
            vin_string = ';'.join(batch.tolist())
            response = requests.post(
                'https://vpic.nhtsa.dot.gov/api/vehicles/DecodeVINValuesBatch/',
                data={'format': 'json', 'data': vin_string}
            )
            if response.status_code == 200:
                data = response.json()['Results']
                for d in data:
                    decoded_data.append(d)
            else:
                print('Error:', response.status_code, response.text)
                break
            # Update the progress bar
            self.progress_bar.setValue((i + 1) * 100 // len(batches))
        # Create a new DataFrame with the decoded data and selected columns
        decoded_df = pd.DataFrame(decoded_data)
        selected_cols = ['VIN'] + [self.df.columns[i] for i in range(len(self.df.columns)) if
                                   self.df.columns[i] in decoded_df.columns]
        output_df = pd.merge(self.df[selected_cols], decoded_df, on='VIN')
        # Show a file dialog to select where to save the output file
        options = QFileDialog.Options()
        options |= QFileDialog.DontUseNativeDialog
        file_name, _ = QFileDialog.getSaveFileName(self, 'Save File', '', 'CSV Files (*.csv)', options=options)
        print(decoded_df.columns)
        # Save the output file
        if file_name:
            output_df.to_csv(file_name, index=False)
        # Reset the progress bar
        self.progress_bar.setValue(0)

    def view_sheet(self):
        # Open the input file in the default application
        file_name = self.file_label.text()
        if file_name:
            import os
            os.startfile(file_name)

if __name__ == '__main__':
    app = QApplication(sys.argv)
    decoder = VINDecoder()
    sys.exit(app.exec_())