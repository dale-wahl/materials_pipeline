import os

class Lab_File_Handler():
    def __init__(self, lab_files_directory):
        self.lab_files_directory = lab_files_directory
        self.lab_filenames = [file for file in os.listdir(self.lab_files_directory) if file[-4:] == '.txt']

        self.files = None

    def collect_lab_files(self):
        """
        Function loops through all files in lab_files_directory and creates a dictionary object from each.

        If the file format of lab files changes, this function will need to be modified respectively.

        Note: this loops through ALL .txt files in the given directory; it expects no other .txt files to be in the directory BUT lab files.
        """
        files = []
        # Loop through filenames collected from lab_files_directory
        for filename in self.lab_filenames:
            temp_dict = {}
            with open(self.lab_files_directory + filename, "r") as file:
                for n, line in enumerate(file):
                    # skip over first two lines (no information in lab file format)
                    if n > 1:
                        # split on tabs
                        line = line.split('\t')
                        # add key and item pair to dictionary
                        temp_dict[line[0].strip()] = line[-1].strip()
            # collect dictionary item
            files.append(temp_dict)
        self.files = files
