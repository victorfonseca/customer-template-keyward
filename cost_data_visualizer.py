#!/usr/bin/env python3
import pandas as pd
import json
import os
import sys
import subprocess
import numpy as np
from datetime import datetime

def install_packages():
    packages = ['matplotlib', 'seaborn', 'scipy', 'minio']
    subprocess.check_call([sys.executable, "-m", "pip", "install"] + packages)
    
    import matplotlib
    matplotlib.use('Agg')
    import matplotlib.pyplot as plt
    import seaborn as sns
    return plt, sns

def get_config():
    dag_key = os.environ.get('DAG_KEY', 'default')
    config_filename = f"cost-of-living-pipeline-{dag_key}.json"
    
    try:
        from minio import Minio
        client = Minio(
            os.environ.get('MINIO_ENDPOINT', 'minio.minio-system.svc.cluster.local:9000'),
            access_key=os.environ.get('AWS_ACCESS_KEY_ID', 'minio'),
            secret_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'minio123'),
            secure=False
        )
        
        if client.bucket_exists('customer-bucket'):
            response = client.get_object('customer-bucket', config_filename)
            config = json.loads(response.read().decode('utf-8'))
            print(f"✅ Config loaded: {config_filename}")
            return config
    except Exception as e:
        print(f"⚠️ Config load failed: {e}")
    
    return {
        "data_fetcher": {"csv_filename": "cost_of_living.csv"},
        "visualizer": {"output_dir": "visualizations", "chart_dpi": 300},
        "minio": {"bucket_name": "customer-bucket", "pipeline_prefix": "cost-of-living"}
    }

def download_from_minio(bucket_name, object_name, local_file_path):
    from minio import Minio
    client = Minio(
        os.environ.get('MINIO_ENDPOINT', 'minio.minio-system.svc.cluster.local:9000'),
        access_key=os.environ.get('AWS_ACCESS_KEY_ID', 'minio'),
        secret_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'minio123'),
        secure=False
    )
    
    os.makedirs(os.path.dirname(local_file_path), exist_ok=True)
    client.fget_object(bucket_name, object_name, local_file_path)
    print(f"✅ Downloaded: {local_file_path}")

def upload_to_minio(local_file_path, bucket_name, object_name):
    from minio import Minio
    client = Minio(
        os.environ.get('MINIO_ENDPOINT', 'minio.minio-system.svc.cluster.local:9000'),
        access_key=os.environ.get('AWS_ACCESS_KEY_ID', 'minio'),
        secret_key=os.environ.get('AWS_SECRET_ACCESS_KEY', 'minio123'),
        secure=False
    )
    
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)
    client.fput_object(bucket_name, object_name, local_file_path)
    print(f"✅ Uploaded: {object_name}")

def analyze_data_structure(df):
    """Comprehensive data structure analysis"""
    # Auto-detect numeric columns (x1, x2, etc. pattern)
    numeric_cols = [col for col in df.columns if col.startswith('x') and col[1:].isdigit()]
    numeric_cols.sort(key=lambda x: int(x[1:]))
    
    # If no x-pattern columns, use all numeric columns
    if not numeric_cols:
        numeric_cols = [col for col in df.columns if df[col].dtype in ['int64', 'float64']]
    
    analysis = {
        'total_rows': len(df),
        'total_columns': len(df.columns),
        'numeric_columns': numeric_cols,
        'categorical_columns': [col for col in df.columns if df[col].dtype == 'object'],
        'cities': df['city'].nunique() if 'city' in df.columns else 0,
        'countries': df['country'].nunique() if 'country' in df.columns else 0,
        'data_quality_range': (df['data_quality'].min(), df['data_quality'].max()) if 'data_quality' in df.columns else (0, 100)
    }
    
    # Calculate variations for all numeric columns
    if numeric_cols:
        variations = {}
        for col in numeric_cols:
            col_mean = df[col].mean()
            if col_mean != 0 and not pd.isna(col_mean):
                variations[col] = df[col].std() / col_mean
            else:
                variations[col] = 0
        
        analysis['most_variable_columns'] = sorted(variations.items(), key=lambda x: x[1], reverse=True)
        
        high_value_cols = []
        for col in numeric_cols:
            mean_val = df[col].mean()
            if not pd.isna(mean_val) and mean_val > 50:
                high_value_cols.append((col, mean_val))
        
        analysis['high_value_columns'] = sorted(high_value_cols, key=lambda x: x[1], reverse=True)
    else:
        analysis['most_variable_columns'] = []
        analysis['high_value_columns'] = []
    
    print(f"📊 Data Analysis Complete:")
    print(f"   • {analysis['total_rows']:,} cities across {analysis['countries']} countries")
    print(f"   • {len(analysis['numeric_columns'])} numeric metrics found")
    print(f"   • Most variable metric: {analysis['most_variable_columns'][0][0] if analysis['most_variable_columns'] else 'None'}")
    
    return analysis

def create_data_overview_plot(df, analysis, output_dir, plt, sns):
    try:
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Dataset Overview', fontsize=16, fontweight='bold')
        
        # Countries distribution
        if 'country' in df.columns:
            top_countries = df['country'].value_counts().head(15)
            axes[0, 0].bar(range(len(top_countries)), top_countries.values)
            axes[0, 0].set_title('Cities per Country (Top 15)')
            axes[0, 0].set_xticks(range(len(top_countries)))
            axes[0, 0].set_xticklabels(top_countries.index, rotation=45, ha='right')
            axes[0, 0].set_ylabel('Number of Cities')
        
        # Data quality distribution
        if 'data_quality' in df.columns:
            axes[0, 1].hist(df['data_quality'], bins=20, alpha=0.7, color='skyblue')
            axes[0, 1].set_title('Data Quality Distribution')
            axes[0, 1].set_xlabel('Data Quality Score')
            axes[0, 1].set_ylabel('Frequency')
        
        # Average values of most variable columns
        if analysis['most_variable_columns']:
            top_cols = [col for col, _ in analysis['most_variable_columns'][:8]]
            means = [df[col].mean() for col in top_cols]
            axes[1, 0].bar(range(len(top_cols)), means, color='lightgreen')
            axes[1, 0].set_title('Mean Values of Most Variable Metrics')
            axes[1, 0].set_xticks(range(len(top_cols)))
            axes[1, 0].set_xticklabels(top_cols, rotation=45)
            axes[1, 0].set_ylabel('Average Value')
        
        # Missing data analysis
        sample_cols = analysis['numeric_columns'][:20] if len(analysis['numeric_columns']) > 20 else analysis['numeric_columns']
        if sample_cols:
            missing_data = df[sample_cols].isnull().sum()
            axes[1, 1].bar(range(len(missing_data)), missing_data.values, color='coral')
            axes[1, 1].set_title('Missing Values by Column')
            axes[1, 1].set_xticks(range(len(missing_data)))
            axes[1, 1].set_xticklabels(missing_data.index, rotation=45)
            axes[1, 1].set_ylabel('Missing Values Count')
        
        plt.tight_layout()
        overview_path = f"{output_dir}/data_overview.png"
        plt.savefig(overview_path, dpi=300, bbox_inches='tight')
        plt.close()
        print("✅ Data overview plot created")
        return overview_path
    except Exception as e:
        print(f"❌ Error creating data overview: {e}")
        return None

def create_city_rankings(df, analysis, output_dir, plt, sns):
    if not analysis['most_variable_columns'] or 'city' not in df.columns:
        return None
    
    try:
        primary_metric = analysis['most_variable_columns'][0][0]
        fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(20, 8))
        
        # Top 20 cities
        top_cities = df.nlargest(20, primary_metric)
        bars1 = ax1.barh(range(len(top_cities)), top_cities[primary_metric])
        ax1.set_yticks(range(len(top_cities)))
        if 'country' in df.columns:
            ax1.set_yticklabels([f"{city} ({country})" for city, country in zip(top_cities['city'], top_cities['country'])])
        else:
            ax1.set_yticklabels(top_cities['city'])
        ax1.set_title(f'Top 20 Cities by {primary_metric}', fontweight='bold')
        ax1.set_xlabel('Value')
        
        # Add value labels
        for i, (bar, value) in enumerate(zip(bars1, top_cities[primary_metric])):
            ax1.text(value + max(top_cities[primary_metric]) * 0.01, i, f'{value:.1f}', va='center', fontsize=9)
        
        # Bottom 20 cities
        bottom_cities = df.nsmallest(20, primary_metric)
        bars2 = ax2.barh(range(len(bottom_cities)), bottom_cities[primary_metric], color='lightcoral')
        ax2.set_yticks(range(len(bottom_cities)))
        if 'country' in df.columns:
            ax2.set_yticklabels([f"{city} ({country})" for city, country in zip(bottom_cities['city'], bottom_cities['country'])])
        else:
            ax2.set_yticklabels(bottom_cities['city'])
        ax2.set_title(f'Bottom 20 Cities by {primary_metric}', fontweight='bold')
        ax2.set_xlabel('Value')
        
        # Add value labels
        for i, (bar, value) in enumerate(zip(bars2, bottom_cities[primary_metric])):
            ax2.text(value + max(bottom_cities[primary_metric]) * 0.01, i, f'{value:.1f}', va='center', fontsize=9)
        
        plt.tight_layout()
        rankings_path = f"{output_dir}/city_rankings.png"
        plt.savefig(rankings_path, dpi=300, bbox_inches='tight')
        plt.close()
        print("✅ City rankings plot created")
        return rankings_path
    except Exception as e:
        print(f"❌ Error creating city rankings: {e}")
        return None

def create_correlation_analysis(df, analysis, output_dir, plt, sns):
    if len(analysis['numeric_columns']) < 2:
        return None
    
    try:
        # Use top 15 most variable columns for correlation
        top_cols = [col for col, _ in analysis['most_variable_columns'][:15]]
        if len(top_cols) < 2:
            top_cols = analysis['numeric_columns'][:15]
        
        corr_matrix = df[top_cols].corr()
        
        plt.figure(figsize=(12, 10))
        if sns:
            mask = np.triu(np.ones_like(corr_matrix, dtype=bool))
            sns.heatmap(corr_matrix, mask=mask, annot=True, cmap='RdYlBu_r', center=0,
                       square=True, linewidths=0.5, cbar_kws={"shrink": .8}, fmt='.2f')
        else:
            im = plt.imshow(corr_matrix, cmap='RdYlBu_r', aspect='auto')
            plt.colorbar(im, shrink=0.8)
            for i in range(len(corr_matrix)):
                for j in range(len(corr_matrix)):
                    plt.text(j, i, f'{corr_matrix.iloc[i, j]:.2f}', ha='center', va='center', fontsize=8)
            plt.xticks(range(len(top_cols)), top_cols, rotation=45)
            plt.yticks(range(len(top_cols)), top_cols)
        
        plt.title('Correlation Matrix of Key Metrics', fontsize=16, fontweight='bold')
        plt.tight_layout()
        correlation_path = f"{output_dir}/correlation_matrix.png"
        plt.savefig(correlation_path, dpi=300, bbox_inches='tight')
        plt.close()
        print("✅ Correlation analysis plot created")
        return correlation_path
    except Exception as e:
        print(f"❌ Error creating correlation analysis: {e}")
        return None

def create_country_analysis(df, analysis, output_dir, plt, sns):
    if 'country' not in df.columns or not analysis['most_variable_columns']:
        return None
    
    try:
        top_countries = df['country'].value_counts().head(10).index
        key_metrics = [col for col, _ in analysis['most_variable_columns'][:4]]
        
        fig, axes = plt.subplots(2, 2, figsize=(16, 12))
        fig.suptitle('Country-Level Analysis', fontsize=16, fontweight='bold')
        
        for i, metric in enumerate(key_metrics):
            ax = axes[i//2, i%2]
            country_means = df[df['country'].isin(top_countries)].groupby('country')[metric].mean().sort_values(ascending=False)
            
            bars = ax.bar(range(len(country_means)), country_means.values, 
                         color=plt.cm.Set3(np.linspace(0, 1, len(country_means))))
            ax.set_title(f'Average {metric} by Country', fontweight='bold')
            ax.set_xticks(range(len(country_means)))
            ax.set_xticklabels(country_means.index, rotation=45, ha='right')
            ax.set_ylabel('Average Value')
            
            for bar, value in zip(bars, country_means.values):
                ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + max(country_means.values) * 0.01, 
                       f'{value:.1f}', ha='center', va='bottom', fontsize=9)
        
        plt.tight_layout()
        country_path = f"{output_dir}/country_analysis.png"
        plt.savefig(country_path, dpi=300, bbox_inches='tight')
        plt.close()
        print("✅ Country analysis plot created")
        return country_path
    except Exception as e:
        print(f"❌ Error creating country analysis: {e}")
        return None

def create_distribution_analysis(df, analysis, output_dir, plt, sns):
    if not analysis['most_variable_columns']:
        return None
    
    try:
        top_cols = [col for col, _ in analysis['most_variable_columns'][:6]]
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        fig.suptitle('Distribution Analysis of Key Metrics', fontsize=16, fontweight='bold')
        
        for i, col in enumerate(top_cols):
            ax = axes[i//3, i%3]
            data = df[col].dropna()
            
            if len(data) > 0:
                ax.hist(data, bins=30, alpha=0.7, density=True, color='skyblue')
                
                # Add KDE curve
                try:
                    from scipy import stats
                    kde = stats.gaussian_kde(data)
                    x_range = np.linspace(data.min(), data.max(), 100)
                    ax.plot(x_range, kde(x_range), 'r-', linewidth=2)
                except:
                    pass
                
                ax.set_title(f'Distribution of {col}', fontweight='bold')
                ax.set_xlabel('Value')
                ax.set_ylabel('Density')
                
                mean_val = data.mean()
                std_val = data.std()
                ax.text(0.65, 0.95, f'Mean: {mean_val:.2f}\nStd: {std_val:.2f}', 
                       transform=ax.transAxes, verticalalignment='top',
                       bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
        
        plt.tight_layout()
        distribution_path = f"{output_dir}/distribution_analysis.png"
        plt.savefig(distribution_path, dpi=300, bbox_inches='tight')
        plt.close()
        print("✅ Distribution analysis plot created")
        return distribution_path
    except Exception as e:
        print(f"❌ Error creating distribution analysis: {e}")
        return None

def create_scatter_matrix(df, analysis, output_dir, plt, sns):
    if len(analysis['most_variable_columns']) < 4:
        return None
    
    try:
        top_cols = [col for col, _ in analysis['most_variable_columns'][:4]]
        fig, axes = plt.subplots(4, 4, figsize=(16, 16))
        fig.suptitle('Scatter Plot Matrix of Key Metrics', fontsize=16, fontweight='bold')
        
        for i, col1 in enumerate(top_cols):
            for j, col2 in enumerate(top_cols):
                ax = axes[i, j]
                
                if i == j:
                    # Diagonal: histogram
                    data = df[col1].dropna()
                    if len(data) > 0:
                        ax.hist(data, bins=20, alpha=0.7, color='lightblue')
                    ax.set_title(f'{col1}', fontsize=10)
                else:
                    # Off-diagonal: scatter plot
                    ax.scatter(df[col2], df[col1], alpha=0.5, s=20)
                    ax.set_xlabel(col2 if i == 3 else '')
                    ax.set_ylabel(col1 if j == 0 else '')
                    
                    # Add correlation coefficient
                    corr = df[[col1, col2]].corr().iloc[0, 1]
                    if not pd.isna(corr):
                        ax.text(0.05, 0.95, f'r={corr:.2f}', transform=ax.transAxes, 
                               verticalalignment='top', fontweight='bold')
        
        plt.tight_layout()
        scatter_path = f"{output_dir}/scatter_matrix.png"
        plt.savefig(scatter_path, dpi=300, bbox_inches='tight')
        plt.close()
        print("✅ Scatter matrix plot created")
        return scatter_path
    except Exception as e:
        print(f"❌ Error creating scatter matrix: {e}")
        return None

def create_comprehensive_html_report(df, analysis, created_files, output_dir):
    """Create comprehensive HTML report with all findings"""
    html_path = f"{output_dir}/comprehensive_cost_analysis.html"
    
    html_content = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>Comprehensive Cost of Living Analysis</title>
        <style>
            body {{ font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 20px; max-width: 1400px; margin: 0 auto; background-color: #f8f9fa; line-height: 1.6; }}
            .header {{ background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; padding: 2rem; border-radius: 10px; margin-bottom: 2rem; text-align: center; }}
            h1 {{ margin: 0; font-size: 2.5rem; }}
            .subtitle {{ font-size: 1.2rem; opacity: 0.9; margin-top: 0.5rem; }}
            .summary-stats {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 1rem; margin-bottom: 2rem; }}
            .stat-card {{ background: white; padding: 1.5rem; border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1); text-align: center; }}
            .stat-number {{ font-size: 2rem; font-weight: bold; color: #667eea; }}
            .stat-label {{ color: #666; font-size: 0.9rem; }}
            .visualization {{ background: white; margin: 2rem 0; border-radius: 10px; box-shadow: 0 4px 15px rgba(0,0,0,0.1); overflow: hidden; }}
            .viz-header {{ background: #667eea; color: white; padding: 1rem 1.5rem; font-size: 1.3rem; font-weight: bold; }}
            .viz-content {{ padding: 1rem; text-align: center; }}
            .viz-content img {{ max-width: 100%; height: auto; border-radius: 5px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }}
            .insights {{ background: #e8f4f8; border-left: 4px solid #667eea; padding: 1rem 1.5rem; margin: 1rem 0; }}
            .insights h3 {{ color: #667eea; margin-top: 0; }}
            table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 10px rgba(0,0,0,0.1); }}
            th, td {{ border: none; padding: 12px 15px; text-align: left; }}
            th {{ background: #667eea; color: white; font-weight: bold; }}
            tr:nth-child(even) {{ background-color: #f8f9fa; }}
            tr:hover {{ background-color: #e3f2fd; }}
            .footer {{ text-align: center; color: #666; margin-top: 3rem; padding: 2rem; border-top: 1px solid #ddd; }}
        </style>
    </head>
    <body>
        <div class="header">
            <h1>Comprehensive Cost of Living Analysis</h1>
            <div class="subtitle">Advanced Analytics Dashboard</div>
        </div>
        
        <div class="summary-stats">
            <div class="stat-card">
                <div class="stat-number">{analysis['total_rows']:,}</div>
                <div class="stat-label">Total Cities</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{analysis['countries']}</div>
                <div class="stat-label">Countries</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{len(analysis['numeric_columns'])}</div>
                <div class="stat-label">Metrics Analyzed</div>
            </div>
            <div class="stat-card">
                <div class="stat-number">{analysis['data_quality_range'][1]:.0f}</div>
                <div class="stat-label">Max Data Quality</div>
            </div>
        </div>
    """
    
    # Add key insights
    if analysis['most_variable_columns']:
        most_variable = analysis['most_variable_columns'][0][0]
        html_content += f"""
        <div class="insights">
            <h3>Key Insights</h3>
            <ul>
                <li><strong>Primary Cost Indicator:</strong> {most_variable} shows the highest variation across cities</li>
                <li><strong>Data Coverage:</strong> Analysis includes {analysis['countries']} countries with {analysis['total_rows']:,} cities</li>
                <li><strong>Quality Score:</strong> Data quality ranges from {analysis['data_quality_range'][0]:.0f} to {analysis['data_quality_range'][1]:.0f}</li>
                <li><strong>Metrics Available:</strong> {len(analysis['numeric_columns'])} different cost-of-living metrics analyzed</li>
            </ul>
        </div>
        """
    
    # Add all visualizations
    viz_titles = {
        'data_overview.png': 'Dataset Overview',
        'city_rankings.png': 'City Rankings',
        'correlation_matrix.png': 'Correlation Analysis',
        'country_analysis.png': 'Country-Level Analysis',
        'distribution_analysis.png': 'Distribution Analysis',
        'scatter_matrix.png': 'Scatter Plot Matrix'
    }
    
    for file_path in created_files:
        if file_path and file_path.endswith('.png'):
            filename = os.path.basename(file_path)
            title = viz_titles.get(filename, filename.replace('.png', '').replace('_', ' ').title())
            html_content += f"""
        <div class="visualization">
            <div class="viz-header">{title}</div>
            <div class="viz-content">
                <img src="{filename}" alt="{title}">
            </div>
        </div>
            """
    
    # Add top cities table if data available
    if analysis['most_variable_columns'] and 'city' in df.columns:
        primary_metric = analysis['most_variable_columns'][0][0]
        top_cities = df.nlargest(20, primary_metric)
        
        html_content += f"""
        <div class="visualization">
            <div class="viz-header">Top 20 Cities by {primary_metric}</div>
            <div class="viz-content">
                <table>
                    <tr>
                        <th>Rank</th>
                        <th>City</th>
                        {"<th>Country</th>" if 'country' in df.columns else ""}
                        <th>{primary_metric}</th>
                        {"<th>Data Quality</th>" if 'data_quality' in df.columns else ""}
                    </tr>
        """
        
        for i, (_, row) in enumerate(top_cities.iterrows(), 1):
            html_content += f"""
                    <tr>
                        <td>{i}</td>
                        <td>{row['city']}</td>
                        {"<td>" + str(row['country']) + "</td>" if 'country' in df.columns else ""}
                        <td>{row[primary_metric]:.2f}</td>
                        {"<td>" + str(row['data_quality']) + "</td>" if 'data_quality' in df.columns else ""}
                    </tr>
            """
        
        html_content += """
                </table>
            </div>
        </div>
        """
    
    html_content += f"""
        <div class="footer">
            <p>Report generated on: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}</p>
            <p>Comprehensive analysis of {analysis['total_rows']:,} cities across {analysis['countries']} countries</p>
        </div>
    </body>
    </html>
    """
    
    with open(html_path, 'w', encoding='utf-8') as f:
        f.write(html_content)
    
    print("✅ Comprehensive HTML report created")
    return html_path

def main():
    print("🎨 Comprehensive Cost Data Visualizer")
    print(f"   🔑 DAG Key: {os.environ.get('DAG_KEY', 'default')}")
    print("=" * 50)
    
    # Install packages
    plt, sns = install_packages()
    
    # Get config (only for operational settings)
    config = get_config()
    dag_key = os.environ.get('DAG_KEY', 'default')
    
    # Setup paths
    bucket_name = config['minio']['bucket_name']
    pipeline_prefix = config['minio']['pipeline_prefix']
    csv_filename = config['data_fetcher']['csv_filename']
    output_dir = config['visualizer']['output_dir']
    
    csv_object = f"{pipeline_prefix}-{dag_key}/data/{csv_filename}"
    local_csv = f"data/{csv_filename}"
    
    try:
        # Download and analyze data
        download_from_minio(bucket_name, csv_object, local_csv)
        df = pd.read_csv(local_csv)
        print(f"✅ Data loaded: {df.shape}")
        
        # Comprehensive data analysis
        analysis = analyze_data_structure(df)
        os.makedirs(output_dir, exist_ok=True)
        
        # Create ALL visualizations automatically
        print("🎨 Creating comprehensive visualizations...")
        created_files = []
        
        viz_functions = [
            create_data_overview_plot,
            create_city_rankings,
            create_correlation_analysis,
            create_country_analysis,
            create_distribution_analysis,
            create_scatter_matrix
        ]
        
        for viz_func in viz_functions:
            result = viz_func(df, analysis, output_dir, plt, sns)
            if result:
                created_files.append(result)
        
        # Create comprehensive HTML report
        html_path = create_comprehensive_html_report(df, analysis, created_files, output_dir)
        if html_path:
            created_files.append(html_path)
        
        # Upload all results to MinIO
        print("☁️ Uploading all results to MinIO...")
        for file_path in created_files:
            filename = os.path.basename(file_path)
            object_name = f"{pipeline_prefix}-{dag_key}/visualizations/{filename}"
            upload_to_minio(file_path, bucket_name, object_name)
        
        print(f"\n🎉 SUCCESS! Created {len(created_files)} comprehensive visualizations")
        return {
            'status': 'success',
            'dag_key': dag_key,
            'files_created': len(created_files),
            'files': [os.path.basename(f) for f in created_files],
            'analysis_summary': {
                'cities': analysis['total_rows'],
                'countries': analysis['countries'],
                'metrics': len(analysis['numeric_columns']),
                'primary_metric': analysis['most_variable_columns'][0][0] if analysis['most_variable_columns'] else None
            }
        }
        
    except Exception as e:
        print(f"\n❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return {'status': 'error', 'message': str(e)}

if __name__ == "__main__":
    results = main()
    print(f"\n📊 Final Results:")
    print(json.dumps(results, indent=2))