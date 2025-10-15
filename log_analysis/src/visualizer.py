import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter
plt.style.use('seaborn-v0_8')



class Visualizer:

    @staticmethod
    def format_large_numbers(x, pos):
        if x >= 1e6:
            return f'{x/1e6:.1f}M'
        elif x >= 1e3:
            return f'{x/1e3:.1f}K'
        else:
            return f'{x:.0f}'

    @staticmethod
    def plot_status_distribution(df):
        pdf = df.toPandas()
        
        fig = plt.figure(figsize=(14, 9))
        
        gs = plt.GridSpec(1, 1, figure=fig)
        ax = fig.add_subplot(gs[0])
        
        colors = []
        for status in pdf["status"]:
            if str(status).startswith('2'): 
                colors.append('#2Ecc71')  
            elif str(status).startswith('3'):  
                colors.append('#3498db')  
            elif str(status).startswith('4'):  
                colors.append('#e74c3c')  
            elif str(status).startswith('5'):  
                colors.append('#f39c12')  
            else:
                colors.append('#95a5a6') 
        
        bars = ax.bar(range(len(pdf)), pdf["count"], 
                     color=colors, alpha=0.85,
                     edgecolor='white', linewidth=2)
        
        for i, bar in enumerate(bars):
            height = bar.get_height()
            ax.text(bar.get_x() + bar.get_width()/2., height + height*0.02,
                   f'{height:,.0f}', 
                   ha='center', va='bottom', 
                   fontsize=14, fontweight='bold',
                   bbox=dict(boxstyle="round,pad=0.2", facecolor="white", alpha=0.9))
        
        ax.set_title("📊 HTTP Status Code Distribution\n", 
                    fontsize=18, fontweight='bold', pad=30, color='#2c3e50')
        
        ax.set_xlabel("Status Code", fontsize=14, fontweight='semibold', labelpad=15)
        ax.set_ylabel("Request Count", fontsize=14, fontweight='semibold', labelpad=15)
        
        ax.set_xticks(range(len(pdf)))
        ax.set_xticklabels(pdf["status"], fontsize=12, fontweight='bold')
        
        ax.yaxis.set_major_formatter(FuncFormatter(Visualizer.format_large_numbers))
        
        ax.grid(axis='y', alpha=0.2, linestyle='-', linewidth=1)
        ax.set_axisbelow(True)
        
        for spine in ax.spines.values():
            spine.set_visible(False)
        
        success_patch = plt.Rectangle((0,0),1,1, fc='#2Ecc71', alpha=0.8)
        redirect_patch = plt.Rectangle((0,0),1,1, fc='#3498db', alpha=0.8)
        client_error_patch = plt.Rectangle((0,0),1,1, fc='#e74c3c', alpha=0.8)
        server_error_patch = plt.Rectangle((0,0),1,1, fc='#f39c12', alpha=0.8)
        
        ax.legend([success_patch, redirect_patch, client_error_patch, server_error_patch],
                 ['2xx: Success', '3xx: Redirect', '4xx: Client Error', '5xx: Server Error'],
                 loc='upper right', framealpha=0.9, bbox_to_anchor=(1, 1))
        
        total_requests = pdf["count"].sum()
        
        fig.text(0.15, 0.92, f'Total Requests: {total_requests:,.0f}', 
                fontsize=16, fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.8", facecolor="lightblue", 
                         edgecolor='navy', alpha=0.9),
                ha='left', va='center')
        
        plt.tight_layout(rect=[0, 0, 1, 0.95])  
        plt.savefig("status_distribution.png", dpi=300, bbox_inches='tight', 
                   facecolor='white', edgecolor='none')
        plt.close()

    @staticmethod
    def plot_requests_per_hour(df):
        pdf = df.toPandas()
        
        fig = plt.figure(figsize=(16, 9))
        ax = fig.add_subplot(111)
        
        ax.plot(pdf["hour"], pdf["count"], 
               marker='o', linewidth=3, markersize=8, 
               color='#FF6B6B', alpha=0.8, markerfacecolor='white', 
               markeredgewidth=2, markeredgecolor='#FF6B6B')
        
        ax.fill_between(pdf["hour"], pdf["count"], alpha=0.3, color='#FF6B6B')
        
        ax.set_title("🕒 Requests per Hour - Traffic Pattern\n", 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel("Hour of Day", fontsize=14, fontweight='bold', labelpad=15)
        ax.set_ylabel("Number of Requests", fontsize=14, fontweight='bold', labelpad=15)
        
        ax.yaxis.set_major_formatter(FuncFormatter(Visualizer.format_large_numbers))
        ax.set_xticks(range(0, 24))
        ax.set_xlim(-0.5, 23.5)
        
        peak_hour = pdf.loc[pdf["count"].idxmax()]
        ax.axvline(x=peak_hour["hour"], color='red', linestyle='--', alpha=0.7,
                  label=f'Peak: Hour {int(peak_hour["hour"])} ({peak_hour["count"]:,.0f} requests)')
        
        ax.tick_params(axis='both', which='major', labelsize=12)
        ax.grid(True, alpha=0.2)
        ax.set_axisbelow(True)
        ax.legend(fontsize=12)
        
        total_requests = pdf["count"].sum()
        fig.text(0.15, 0.92, f'Total Requests: {total_requests:,.0f}', 
                fontsize=14, fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.6", facecolor="lightgreen", 
                         edgecolor='green', alpha=0.9),
                ha='left', va='center')
        
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.savefig("requests_per_hour.png", dpi=300, bbox_inches='tight')
        plt.close()

    @staticmethod
    def plot_top_paths(df):
        pdf = df.toPandas()
        
        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(111)
        
        pdf = pdf.sort_values('count', ascending=True)
        
        bars = ax.barh(pdf["path"], pdf["count"], 
                      color='#9B59B6', alpha=0.8,
                      edgecolor='white', linewidth=1.5)
        
        for i, bar in enumerate(bars):
            width = bar.get_width()
            ax.text(width + width*0.01, bar.get_y() + bar.get_height()/2.,
                   f'{width:,.0f}', ha='left', va='center', 
                   fontsize=11, fontweight='bold')
        
        ax.set_title("🔝 Top Requested Paths\n", 
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel("Total Requests", fontsize=14, fontweight='bold', labelpad=15)
        
        ax.xaxis.set_major_formatter(FuncFormatter(Visualizer.format_large_numbers))
        
        labels = [label.get_text()[:50] + '...' if len(label.get_text()) > 50 
                 else label.get_text() for label in ax.get_yticklabels()]
        ax.set_yticklabels(labels)
        
        ax.tick_params(axis='both', which='major', labelsize=11)
        ax.grid(axis='x', alpha=0.2)
        ax.set_axisbelow(True)
        
        total_requests = pdf["count"].sum()
        fig.text(0.15, 0.92, f'Total Requests: {total_requests:,.0f}', 
                fontsize=14, fontweight='bold',
                bbox=dict(boxstyle="round,pad=0.6", facecolor="lightyellow", 
                         edgecolor='orange', alpha=0.9),
                ha='left', va='center')
        
        plt.tight_layout(rect=[0, 0, 1, 0.95])
        plt.savefig("top_paths.png", dpi=300, bbox_inches='tight')
        plt.close()