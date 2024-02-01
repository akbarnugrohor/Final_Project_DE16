import pandas as pd 
from sqlalchemy.exc import SQLAlchemyError
import numpy as np

class Transformation():
    def __init__(self, engine_sql, engine_postgres):
        self.engine_sql = engine_sql
        self.engine_postgres = engine_postgres
    
    def get_data_from_mysql(self):
        sql = "SELECT * FROM covid_jawa_barat"
        df = pd.read_sql(sql, con=self.engine_sql)
        print('Get Data Success!')
        return df
    
    def create_dim_province(self):
        df = self.get_data_from_mysql()
        df_province = df[['kode_prov', 'nama_prov']]
        df_province = df_province.rename(columns={'kode_prov':'province_id', 'nama_prov':'province_name'})
        df_province = df_province.drop_duplicates()

        try:
            p = "DROP TABLE IF EXISTS dim_province"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)

        # insert to postgres
        df_province.to_sql(con=self.engine_postgres, name='dim_province', index=False)

        print("Data Inserted Successfully")
    
    def create_dim_district(self):
        df = self.get_data_from_mysql()
        df_district = df[['kode_kab', 'kode_prov', 'nama_kab']]
        df_district = df_district.rename(columns={'kode_kab':'district_id', 'kode_prov':'province_id', 'nama_kab':'district_name'})
        df_district = df_district.drop_duplicates()

        try:
           p = "DROP TABLE IF EXISTS dim_district"
           self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)

         # insert to postgres
        df_district.to_sql(con=self.engine_postgres, name='dim_district', index=False)

        print("Data Inserted Successfully")
    
    def create_dim_case(self):
        df = self.get_data_from_mysql()

        column_start = ['suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'closecontact_meninggal', 'probable_meninggal']

        column_end = ['id', 'status_name', 'status_detail', 'status']

        df = df[column_start]
        df = df[:1]
        df = df.melt(var_name = "status", value_name = "total")
        df = df.drop_duplicates("status").sort_values("status")

        df['id'] = np.arange(1, df.shape[0]+1)
        df[['status_name', 'status_detail']] = df["status"].str.split('_', n=1, expand=True)

        df = df[column_end]

        try:
            p = "DROP TABLE IF EXISTS dim_case"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)
        
         # insert to postgres
        df.to_sql(con=self.engine_postgres, name='dim_case', index=False, if_exists='replace')

        print("Data Inserted Successfully")

        return df
    
    def create_province_daily(self):
        df = self.get_data_from_mysql()
        df_case_dim = self.create_dim_case()

        column_start = ['tanggal', 'kode_prov', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'closecontact_meninggal', 'probable_meninggal']

        column_end = ['date', 'province_id', 'status', 'total']

        data = df[column_start]
        data = data.melt(id_vars = ['tanggal', 'kode_prov'], var_name='status', value_name='total').sort_values(['tanggal', 'kode_prov', 'status', 'total'])
        data = data.groupby(by=['tanggal', 'kode_prov', 'status']).sum()
        data = data.reset_index()

        data.columns = column_end
        data['id'] = np.arange(1, data.shape[0]+1)
        df_case_dim = df_case_dim.rename({'id' : 'case_id'}, axis=1)

        data = pd.merge(data, df_case_dim, how='inner', on='status')
        data = data[['id', 'province_id', 'case_id', 'date', 'total']]

        try:
            p = "DROP TABLE IF EXISTS province_daily"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)
        
         # insert to postgres
        data.to_sql(con=self.engine_postgres, name='province_daily', index=False)

    def create_district_daily(self):
        df = self.get_data_from_mysql()
        df_case_dim = self.create_dim_case()

        column_start = ['tanggal', 'kode_kab', 'suspect_diisolasi', 'suspect_discarded', 'closecontact_dikarantina', 'closecontact_discarded', 'probable_diisolasi', 'probable_discarded', 'confirmation_sembuh', 'confirmation_meninggal', 'closecontact_meninggal', 'probable_meninggal']

        column_end = ['date', 'district_id', 'status', 'total']

        data = df[column_start]
        data = data.melt(id_vars = ['tanggal','kode_kab'], var_name='status', value_name='total').sort_values(['tanggal','kode_kab','status','total'])
        data = data.groupby(by=['tanggal','kode_kab', 'status']).sum()
        data = data.reset_index()

        data.columns = column_end
        data['id'] = np.arange(1, data.shape[0]+1)
        df_case_dim = df_case_dim.rename({'id' : 'case_id'}, axis=1)

        data = pd.merge(data, df_case_dim, how='inner', on='status')
        data = data[['id', 'district_id', 'case_id', 'date', 'total']]

        try:
            p = "DROP TABLE IF EXISTS district_daily"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)
        
         # insert to postgres
        data.to_sql(con=self.engine_postgres, name='district_daily', index=False)

    def create_province_monthly(self):
        sql = """
        select 
        row_number() over () as id,
        province_id, case_id, 
        to_char("date"::date ,'YYYY-MM' ) as "month", 
        sum(total) as total
        from province_daily 
        group by province_id , case_id, to_char("date"::date ,'YYYY-MM');
        """
        data = pd.read_sql(sql, con=self.engine_postgres)

        try:
            p = "DROP TABLE IF EXISTS province_monthly"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)
        
         # insert to postgres
        data.to_sql(con=self.engine_postgres, name='province_monthly', index=False)

    def create_province_yearly(self):
        sql = """
        select 
        row_number() over () as id,
        province_id, case_id, 
        left("month" ,'4' ) as "year", 
        sum(total) as total
        from province_monthly 
        group by province_id , case_id, left("month" ,'4' );
        """
        data = pd.read_sql(sql, con=self.engine_postgres)

        try:
            p = "DROP TABLE IF EXISTS province_yearly"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)
        
         # insert to postgres
        data.to_sql(con=self.engine_postgres, name='province_yearly', index=False)

    def create_district_monthly(self):
        sql = """
        select 
        row_number() over () as id,
        district_id, case_id, 
        to_char("date"::date ,'YYYY-MM' ) as "month", 
        sum(total) as total
        from district_daily 
        group by district_id , case_id, to_char("date"::date ,'YYYY-MM');
        """
        data = pd.read_sql(sql, con=self.engine_postgres)

        try:
            p = "DROP TABLE IF EXISTS district_monthly"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)
        
         # insert to postgres
        data.to_sql(con=self.engine_postgres, name='district_monthly', index=False)

    def create_district_yearly(self):
        sql = """
        select 
        row_number() over () as id,
        district_id, case_id, 
        left("month" ,'4' ) as "year", 
        sum(total) as total
        from district_monthly 
        group by district_id , case_id, left("month" ,'4' );
        """
        data = pd.read_sql(sql, con=self.engine_postgres)

        try:
            p = "DROP TABLE IF EXISTS district_yearly"
            self.engine_postgres.execute(p)
        except SQLAlchemyError as e:
            print(e)
        
         # insert to postgres
        data.to_sql(con=self.engine_postgres, name='district_yearly', index=False)
        