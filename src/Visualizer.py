class Visualizer:

    #init
    def __init__(self, background_image_path):
        self.background_image_path = background_image_path
    
    #create_heatmap
    def create_heatmap(self, df, output_path):
        if df.empty:
            print("DataFrame is empty. Cannot create heatmap.")
            return
        
        # Filter out any points that are off the rink
        df = df[(df['x_coord'] >= -100) & (df['x_coord'] <= 100) & (df['y_coord'] >= -42.5) & (df['y_coord'] <= 42.5)]
        
        # Increase figure size for better resolution
        plt.figure(figsize=(15, 8), dpi=600)  # Use a larger figure size and higher DPI
        img = mpimg.imread(self.background_image_path)
        plt.imshow(img, extent=[-100, 100, -42.5, 42.5])
        
        # Plot goals and misses with different colors
        goals_df = df[df['Play Type'] == 'GOAL']
        misses_df = df[df['Play Type'] == 'MISS']

        plt.scatter(goals_df['x_coord'], goals_df['y_coord'], c='gold', edgecolors='black', label='Goals', zorder=2)
        plt.scatter(misses_df['x_coord'], misses_df['y_coord'], c='silver', alpha=0.7, label='Misses', zorder=1)
        
        # Set the aspect of the plot to be equal
        plt.gca().set_aspect('equal', adjustable='box')
        
        # Remove axes for a cleaner look
        plt.axis('off')
        
        # Create a legend and position it at the bottom of the plot
        legend_elements = [
            mpatches.Patch(color='gold', label='Goals'),
            mpatches.Patch(color='silver', label='Misses'),
        ]
        plt.legend(handles=legend_elements, loc='lower center', bbox_to_anchor=(0.5, -0.05), ncol=2, frameon=False)

        # Save the figure with high DPI and tight bounding box
        plt.savefig(output_path, dpi=600, bbox_inches='tight')
        plt.close()
        print(f"Heatmap saved as '{output_path}'")
    
    #create_seasonal_heatmap
    def create_seasonal_heatmap(self, df, season, output_folder):
        season_df = df[df['Season'] == season]
        if season_df.empty:
            print(f"No data for season {season}. Skipping heatmap generation.")
            return

        heatmap_filename = f'{output_folder}/heatmap_season_{season}.png'
        self.create_heatmap(season_df, heatmap_filename)

    #animate_heatmaps
    def animate_heatmaps(self, seasons, output_folder):
        fig, ax = plt.subplots(figsize=(10, 7))
        fig.subplots_adjust(top=0.85)
        img = mpimg.imread(self.background_image_path)
        ax.imshow(img, extent=[-100, 100, -42.5, 42.5], aspect='auto')
        ax.axis('off')

        def animate(season):
            ax.clear()
            ax.imshow(img, extent=[-100, 100, -42.5, 42.5], aspect='auto')
            heatmap_path = f'{output_folder}/heatmap_season_{season}.png'

            if os.path.exists(heatmap_path):
                heatmap_img = mpimg.imread(heatmap_path)
                ax.imshow(heatmap_img, extent=[-100, 100, -42.5, 42.5], aspect='auto')
            else:
                print(f"Heatmap for season {season} not found.")

            ax.axis('off')
            plt.title(f"Season {season}", fontsize=16)

        anim = FuncAnimation(fig, animate, frames=seasons, interval=800, repeat_delay=2000)

        if not os.path.exists(output_folder):
            os.makedirs(output_folder)

        anim.save(f'{output_folder}/seasons_heatmap_animation.gif', writer='pillow', fps=4)
        plt.close()
