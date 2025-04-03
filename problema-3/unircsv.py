import os
import pandas as pd

def concatenate_all_csvs(data_dir):
    """
    Combina todos los archivos CSV del directorio, agregando todas las columnas
    excepto 'description' y 'tagline' de movies.csv
    
    Args:
        data_dir: Directorio donde se encuentran los archivos CSV
        
    Returns:
        DataFrame combinado de Pandas
    """
    # Listar todos los archivos CSV en el directorio
    csv_files = [f for f in os.listdir(data_dir) if f.endswith('.csv')]
    
    if not csv_files:
        raise ValueError("No se encontraron archivos CSV en el directorio especificado")
    
    print(f"📁 Archivos CSV encontrados ({len(csv_files)}):")
    for f in csv_files:
        print(f" - {f}")
    
    # Cargar todos los DataFrames con tratamiento especial para movies.csv
    dfs = {}
    for file in csv_files:
        try:
            df_name = file.replace('.csv', '')
            file_path = os.path.join(data_dir, file)
            
            # Tratamiento especial para movies.csv
            if file == 'movies.csv':
                # Leer el archivo excluyendo columnas no deseadas
                df = pd.read_csv(file_path)
                if 'description' in df.columns:
                    df = df.drop(columns=['description'])
                if 'tagline' in df.columns:
                    df = df.drop(columns=['tagline'])
                if 'minute' in df.columns:
                    df = df.drop(columns=['minute'])
                dfs[df_name] = df
            else:
                # Cargar normalmente otros archivos
                dfs[df_name] = pd.read_csv(file_path)
            
            print(f"\n✅ {file} cargado correctamente")
            print(f"   Dimensiones: {dfs[df_name].shape[0]} filas × {dfs[df_name].shape[1]} columnas")
            print(f"   Columnas: {list(dfs[df_name].columns)}")
        except Exception as e:
            print(f"\n⚠️ Error al cargar {file}: {str(e)}")
            continue
    
    if not dfs:
        raise ValueError("No se pudo cargar ningún archivo CSV")
    
    # Combinar todos los DataFrames horizontalmente (axis=1)
    combined_df = pd.concat(dfs.values(), axis=1)
    
    # Limpieza básica
    combined_df = combined_df.loc[:, ~combined_df.columns.duplicated()]
    
    return combined_df

def main():
    # Configuración
    data_dir = 'imdb/'
    
    try:
        print(f"\n🔄 Procesando archivos en {data_dir}...")
        final_df = concatenate_all_csvs(data_dir)
        
        # Resultados
        print("\n🎉 Combinación completada con éxito!")
        print(f"📊 Total de filas: {len(final_df)}")
        print(f"📝 Total de columnas: {len(final_df.columns)}")
        print("\n🔍 Columnas resultantes:")
        for i, col in enumerate(final_df.columns, 1):
            print(f"{i}. {col}")
        
        # Guardar resultados
        output_file = os.path.join(data_dir, 'combined_all_data.csv')
        final_df.to_csv(output_file, index=False)
        print(f"\n💾 Datos guardados en: {output_file}")
        
    except Exception as e:
        print(f"\n❌ Error: {str(e)}")

if __name__ == "__main__":
    main()