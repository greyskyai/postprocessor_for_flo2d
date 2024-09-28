import os
import pandas as pd
import matplotlib.pyplot as plt

def extract_variables(file_path):
    with open(file_path, 'r') as file:
        lines = file.readlines()
    
    # Extracting the first two lines of variables
    IRAINREAL, IRAINBUILDING = map(int, lines[0].strip().split())
    RTT, RAINABS, RAINARF, MOVINGSTORM = map(float, lines[1].strip().split())
    
    return {
        'IRAINREAL': IRAINREAL,
        'IRAINBUILDING': IRAINBUILDING,
        'RTT': RTT,
        'RAINABS': RAINABS,
        'RAINARF': RAINARF,
        'MOVINGSTORM': MOVINGSTORM
    }

def extract_time_series_data(file_path):
    data = {
        'Time (hours)': [],
        'Percentage of Total Rainfall Depth (RTT)': []
    }
    
    with open(file_path, 'r') as file:
        for line in file:
            if line.startswith('R'):
                parts = line.strip().split()
                data['Time (hours)'].append(float(parts[1]))
                data['Percentage of Total Rainfall Depth (RTT)'].append(float(parts[2]))
    
    return pd.DataFrame(data)

def save_to_excel(df, variables, output_path):
    with pd.ExcelWriter(output_path) as writer:
        df.to_excel(writer, index=False, sheet_name='Rainfall Data')
        
        workbook = writer.book
        worksheet = writer.sheets['Rainfall Data']
        
        chart = workbook.add_chart({'type': 'line'})
        
        chart.add_series({
            'name':       'Rainfall Percentage',
            'categories': ['Rainfall Data', 1, 0, len(df), 0],
            'values':     ['Rainfall Data', 1, 1, len(df), 1],
            'line':       {'color': 'blue'}
        })
        
        chart.set_title({'name': 'Cumulative Rainfall'})
        chart.set_x_axis({'name': 'Time (hours)'})
        chart.set_y_axis({'name': 'Rainfall Depth Percentage'})
        
        label = f'Total Rainfall Depth: {variables["RTT"]} in.'
        chart.set_legend({'text': label, 'position': 'bottom'})
        
        worksheet.insert_chart('D2', chart)

def save_to_pdf(df, variables, output_path):
    plt.figure(figsize=(8.5, 11))
    plt.plot(df['Time (hours)'], df['Percentage of Total Rainfall Depth (RTT)'], color='blue', label='Rainfall Percentage')
    plt.title('Cumulative Rainfall')
    plt.xlabel('Time (hours)')
    plt.ylabel('Rainfall Depth Percentage')
    label = f'Total Rainfall Depth: {variables["RTT"]} in.'
    plt.text(0.05, 0.95, label, ha='left', va='top', transform=plt.gca().transAxes, fontsize=10, bbox=dict(facecolor='white', alpha=0.6))
    plt.legend(loc='upper left')
    plt.savefig(output_path)

def rain_spreadsheet_and_plot(folder_path):
    rain_file_path = os.path.join(folder_path, 'RAIN.DAT')
    outpath = os.path.join(folder_path, 'flo2d_plots')
    excel_output_path = os.path.join(outpath, 'rainfall_data.xlsx')
    pdf_output_path = os.path.join(outpath, 'rainfall_data.pdf')
    
    variables = extract_variables(rain_file_path)
    df = extract_time_series_data(rain_file_path)
    
    save_to_excel(df, variables, excel_output_path)
    save_to_pdf(df, variables, pdf_output_path)

# Example usage
#folder_path = r'S:\21002795 - Lake Havasu\Project Documents\Engineering-Planning-Power and Energy\Reports\CLOMR\Models\FLO2D\PROP\20240521_PROP_Riprap_n_values_V2'
#rain_spreadsheet_and_plot(folder_path)