import matplotlib.pyplot as plt
from matplotlib.ticker import FuncFormatter, MaxNLocator
import matplotlib.cm as cm
import matplotlib.colors as colors

plt.style.use('seaborn-v0_8')



class Visualizer:

    @staticmethod
    def format_large_numbers(x, pos):
        if x >= 1_000_000:
            return f'{int(x/1_000_000):,}M'
        elif x >= 1_000:
            return f'{int(x/1_000):,}K'
        else:
            return f'{int(x):,}'



    @staticmethod
    def plot_transaction_summary(df):
        pdf = df.toPandas().sort_values("total_amount")


        fig, ax = plt.subplots(figsize=(14,8))
        ax.barh(pdf["pattern_type"], pdf["total_amount"], color="#3498db", alpha=0.8)

        ax.set_title("Transaction Amount by Pattern Type")
        ax.set_xlabel("Total Amount")
        ax.xaxis.set_major_formatter(FuncFormatter(Visualizer.format_large_numbers))
        
        ax.tick_params(axis='y', labelsize=7)
        plt.tight_layout()
        plt.savefig("transaction_summary.png", dpi=300)
        plt.close()



    @staticmethod
    def plot_pattern_summary(df):
        pdf = df.toPandas().sort_values("flagged_count")
        fig, ax = plt.subplots(figsize=(14,8))

        ax.barh(pdf["pattern_type"], pdf["flagged_count"], color="#e74c3c", alpha=0.8)
        ax.set_title("Suspicious Transactions by Pattern Type")
        ax.set_xlabel("Flagged Count")
        ax.xaxis.set_major_formatter(FuncFormatter(Visualizer.format_large_numbers))

        ax.tick_params(axis='y', labelsize=7)
        plt.tight_layout()
        plt.savefig("pattern_summary.png", dpi=300)
        plt.close()


    @staticmethod
    def plot_top_accounts(df):

        pdf = df.toPandas().sort_values("total_sent", ascending=False)
        fig, ax = plt.subplots(figsize=(14,8))
        ax.bar(pdf["sender"], pdf["total_sent"], color="#9b59b6", alpha=0.8)
        ax.set_title("Top Accounts by Total Sent Amount")

        ax.set_ylabel("Total Sent")
        ax.yaxis.set_major_formatter(FuncFormatter(Visualizer.format_large_numbers))
        plt.xticks(rotation=45, ha='right')
        plt.tight_layout()
        plt.savefig("top_accounts.png", dpi=300)
        plt.close()



    @staticmethod
    def plot_hourly_activity(df):
        pdf = df.toPandas()

        norm = colors.Normalize(vmin=pdf["txn_count"].min(), vmax=pdf["txn_count"].max())
        cmap = cm.get_cmap("Reds")

        bar_colors = [cmap(norm(v)) for v in pdf["txn_count"]]

        fig, ax = plt.subplots(figsize=(14, 8))
        bars = ax.bar(pdf["hour"], pdf["txn_count"], color=bar_colors, edgecolor="black", linewidth=0.5)

        ax.set_title("Transactions per Hour", fontsize=16, fontweight="bold")
        ax.set_xlabel("Hour of Day", fontsize=12)
        ax.set_ylabel("Transaction Count", fontsize=12)

        ax.yaxis.set_major_formatter(FuncFormatter(Visualizer.format_large_numbers))
        ax.xaxis.set_major_locator(MaxNLocator(integer=True))
        ax.set_xticks(range(0, 24))

        plt.grid(alpha=0.3)
        plt.tight_layout()

        sm = plt.cm.ScalarMappable(cmap=cmap, norm=norm)
        sm.set_array([])
        cbar = plt.colorbar(sm, ax=ax)
        cbar.set_label("Transaction Volume", fontsize=12)

        plt.savefig("transactions_per_hour.png", dpi=300)
        plt.close()