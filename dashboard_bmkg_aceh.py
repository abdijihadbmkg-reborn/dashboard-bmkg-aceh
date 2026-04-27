import streamlit as st
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import numpy as np
import boto3
from botocore import UNSIGNED
from botocore.config import Config
from satpy import Scene
from pyresample import create_area_def
import matplotlib.pyplot as plt
import matplotlib.colors as mcolors
import matplotlib.patches as mpatches
import cartopy.crs as ccrs        
import datetime
import os
import matplotlib.image as mpimg 
from matplotlib.offsetbox import OffsetImage, AnnotationBbox 
import plotly.express as px
import plotly.graph_objects as go
import time
import matplotlib

# Memaksa matplotlib menggunakan backend non-GUI
matplotlib.use('Agg')

# ==========================================
# 1. KONFIGURASI HALAMAN 
# ==========================================
st.set_page_config(layout="wide", page_title="Dashboard Multi-Bencana BMKG", page_icon="🌍")

# --- SISTEM KEAMANAN (LOGIN) PALING AMAN & ANTI-CRASH ---
if "login_sukses" not in st.session_state:
    st.session_state["login_sukses"] = False

if not st.session_state["login_sukses"]:
    st.markdown("<h3 style='text-align: center;'>🔒 Dasbor Internal BMKG</h3>", unsafe_allow_html=True)
    tebakan = st.text_input("Masukkan PIN / Password untuk mengakses Dasbor:", type="password")
    
    if tebakan:
        # Kita hardcode password di sini agar tidak tergantung pada st.secrets Cloud yang sering error
        if tebakan == "bmkgaceh123":
            st.session_state["login_sukses"] = True
            # Proteksi jika server menggunakan Streamlit versi lama/baru
            if hasattr(st, 'rerun'):
                st.rerun()
            else:
                st.experimental_rerun()
        else:
            st.error("❌ Password salah. Silakan coba lagi.")
    
    # Memblokir semua kode di bawah ini jika belum login
    st.stop()


# --- CSS Styling Dasar ---
st.markdown("""
    <style>
    .reportview-container .main .block-container{ padding-top: 1rem; }
    [data-testid="stPlotlyChart"] { position: relative; }
    </style>
    """, unsafe_allow_html=True)


# ==========================================
# 2. FUNGSI-FUNGSI UTILITY (GEMPA & CUACA)
# ==========================================

# -- FUNGSI GEMPA --
def load_data_gempa():
    try:
        df = pd.read_csv("db_gempa_aceh_lengkap.csv")
        df['Waktu'] = pd.to_datetime(df['Waktu'])
        df = df.sort_values(by='Waktu', ascending=False).reset_index(drop=True)
        df['Bulan'] = df['Waktu'].dt.strftime('%B %Y')
        def ekstrak_kabupaten(lokasi):
            x = str(lokasi).split(' ')[-1]
            return x.replace('KAB-', '').replace('-ACEH', '').replace('KOTA-', '').replace('-', ' ').title().strip()
        df['Kabupaten/Kota'] = df['Lokasi'].apply(ekstrak_kabupaten)
        def tentukan_kategori(d):
            if d <= 60: return 'Dangkal (<= 60 km)'
            elif d <= 300: return 'Menengah (61 - 300 km)'
            else: return 'Dalam (> 300 km)'
        df['Kategori Kedalaman'] = df['Kedalaman'].apply(tentukan_kategori)
        return df
    except Exception as e:
        st.error(f"Gagal memuat data gempa: {e}")
        return pd.DataFrame()

# -- FUNGSI CUACA --
THRESHOLD_TEMP = -33 
RADIUS_MELUAS = 0.15 
DURASI_PERINGATAN = 2 

KOLOM_KABUPATEN = "NAME_2" 
KOLOM_KECAMATAN = "NAME_3" 
NAMA_FILE_GEOJSON = "batas_kecamatan_aceh.geojson"
NAMA_FILE_WATERMARK = "logo_bmkg.png" 

bmkg_bounds = [-100, -80, -75, -69, -62, -56, -48, -41, -34, -28, -21, -13, -7, 0, 8, 14, 21, 60]
bmkg_colors = ['#a50021', '#ff0000', '#ff6666', '#ffb3b3', '#ffe6cc', '#ff9900', '#ffcc00', 
               '#ffff00', '#ccff00', '#00ff00', '#00cc00', '#009900', '#00ffff', '#3399ff', 
               '#0066ff', '#0000cc', '#000000']
cmap_bmkg = mcolors.ListedColormap(bmkg_colors)
norm_bmkg = mcolors.BoundaryNorm(bmkg_bounds, cmap_bmkg.N)

COLOR_EKSTREM = '#FF0000' 
COLOR_POTENSI = '#FFFF00' 
COLOR_AMAN = '#F5F5F5'

def add_central_watermark(ax, image_path, alpha=0.15, zoom=0.6):
    if os.path.exists(image_path):
        logo_image = mpimg.imread(image_path)
        logo_offset = OffsetImage(logo_image, zoom=zoom, alpha=alpha)
        logo_box = AnnotationBbox(logo_offset, (0.5, 0.5), xycoords='axes fraction', frameon=False)
        logo_box.set_zorder(2) 
        ax.add_artist(logo_box)

def add_north_arrow_to_fig(fig, ax, location=(0.92, 0.92), size=0.06, color='black'):
    ax_bbox = ax.get_position()
    pos_x = ax_bbox.x0 + location[0] * ax_bbox.width
    pos_y = ax_bbox.y0 + location[1] * ax_bbox.height
    arrow_ax = fig.add_axes([pos_x - size/2, pos_y - size/2, size, size], projection=ccrs.PlateCarree())
    arrow_ax.axis('off')
    x = [0.5, 0.6, 0.5, 0.4, 0.5]
    y = [0.0, 0.8, 1.0, 0.8, 0.0]
    polygon = mpatches.Polygon(list(zip(x, y)), color=color)
    arrow_ax.add_patch(polygon)
    arrow_ax.text(0.5, 1.1, 'U', transform=arrow_ax.transAxes, ha='center', va='bottom', fontsize=8, color=color, fontweight='bold')

@st.cache_data
def load_batas_kecamatan_aceh():
    if os.path.exists(NAMA_FILE_GEOJSON):
        try:
            return gpd.read_file(NAMA_FILE_GEOJSON).to_crs("EPSG:4326")
        except: return None
    return None

def get_latest_s3_files():
    now = datetime.datetime.now(datetime.timezone.utc)
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    for i in range(1, 11): 
        check_time = now - datetime.timedelta(minutes=i*10)
        year, month, day = check_time.strftime('%Y'), check_time.strftime('%m'), check_time.strftime('%d')
        hour_min = check_time.strftime('%H') + f"{(check_time.minute // 10) * 10:02d}"
        prefix = f"AHI-L1b-FLDK/{year}/{month}/{day}/{hour_min}/"
        try:
            objs = s3.list_objects_v2(Bucket="noaa-himawari9", Prefix=prefix)
            files = [f"s3://noaa-himawari9/{obj['Key']}" for obj in objs.get('Contents', []) if 'B13' in obj['Key']]
            if files: return sorted(files), check_time
        except: continue
    return None, None

@st.cache_data(ttl=600)
def proses_citra_satelit():
    files, waktu_data = get_latest_s3_files()
    if not files: return "FILE_NOT_FOUND", None, None, None, None
    try:
        scn = Scene(reader="ahi_hsd", filenames=files, reader_kwargs={'storage_options': {'anon': True}})
        scn.load(['B13'])
        extent = [94.5, 2.0, 98.5, 6.0] 
        area_def = create_area_def('aceh_zoom', '+proj=latlong', area_extent=extent, resolution=0.02)
        scn_res = scn.resample(area_def)
        lons, lats = scn_res['B13'].attrs['area'].get_lonlats()
        data_bt = scn_res['B13'].values - 273.15
        suhu_min = np.min(data_bt)
        mask_bahaya = data_bt <= THRESHOLD_TEMP
        return data_bt, waktu_data, suhu_min, lons[mask_bahaya], lats[mask_bahaya]
    except Exception as e: return str(e), None, None, None, None

def susun_hierarki_wilayah(df):
    if df.empty: return {}
    grouped = df.groupby(KOLOM_KABUPATEN)[KOLOM_KECAMATAN].unique().to_dict()
    return {kab: sorted(list(kecs)) for kab, kecs in sorted(grouped.items())}

@st.cache_data
def get_wilayah_terdampak(lons_bhy, lats_bhy, _aceh_map_gdf):
    if len(lons_bhy) == 0 or _aceh_map_gdf is None: return None, None, "Data tidak tersedia."
    geom_titik = [Point(xy) for xy in zip(lons_bhy, lats_bhy)]
    gdf_titik = gpd.GeoDataFrame(geometry=geom_titik, crs="EPSG:4326")
    utama_join = gpd.sjoin(gdf_titik, _aceh_map_gdf, predicate='within')
    gdf_buffer = gpd.GeoDataFrame(geometry=gdf_titik.geometry.buffer(RADIUS_MELUAS), crs="EPSG:4326")
    meluas_join = gpd.sjoin(gdf_buffer, _aceh_map_gdf, predicate='intersects')
    
    dict_utama = susun_hierarki_wilayah(utama_join)
    dict_meluas_raw = susun_hierarki_wilayah(meluas_join)
    dict_meluas = {kab: [k for k in kecs if k not in dict_utama.get(kab, [])] 
                   for kab, kecs in dict_meluas_raw.items() if [k for k in kecs if k not in dict_utama.get(kab, [])]}
    return dict_utama, dict_meluas, ""

def format_teks_peringatan(dict_utama, dict_meluas, waktu_data):
    waktu_lokal = waktu_data + datetime.timedelta(hours=7)
    waktu_berakhir = waktu_lokal + datetime.timedelta(hours=DURASI_PERINGATAN)
    teks = f"🕒 *Update: {waktu_lokal.strftime('%d %B %Y, %H:%M WIB')}*\n\n"
    teks += "🔴 **TERDAMPAK SAAT INI:**\n"
    for kab, kecs in dict_utama.items(): teks += f"• *{kab}:* {', '.join(kecs)}\n"
    if dict_meluas:
        teks += "\n🟡 **POTENSI MELUAS KE:**\n"
        for kab, kecs in dict_meluas.items(): teks += f"• *{kab}:* {', '.join(kecs)}\n"
    teks += f"\n⏳ *Berlaku hingga pkl {waktu_berakhir.strftime('%H:%M WIB')}*.\n"
    return teks


# ==========================================
# 3. MENU SIDEBAR (JENDELA NAVIGASI)
# ==========================================
st.sidebar.image("https://www.bmkg.go.id/asset/img/logo/logo-bmkg.png", width=100)
st.sidebar.title("Navigasi Dashboard")
st.sidebar.markdown("---")

pilihan_menu = st.sidebar.radio(
    "Pilih Modul Monitoring:",
    ("📡 Gempa Bumi Real-Time", "⚡ Cuaca Ekstrem (Nowcasting)")
)

st.sidebar.markdown("---")
st.sidebar.info("Dashboard ini menampilkan pemantauan multi-bencana secara real-time untuk wilayah Aceh dan sekitarnya.")


# ==========================================
# 4. TAMPILAN KONTEN BERDASARKAN PILIHAN
# ==========================================

if pilihan_menu == "📡 Gempa Bumi Real-Time":
    st.title("🛰️ Monitoring Real-Time Gempa Bumi Wilayah Aceh")

    df_asal = load_data_gempa()

    if not df_asal.empty:
        list_bulan = ["Semua Data"] + sorted(list(df_asal['Bulan'].unique()), reverse=True)
        pilihan_bulan = st.selectbox("📅 Pilih Periode Data (Peta & Statistik akan menyesuaikan):", list_bulan)

        if pilihan_bulan == "Semua Data":
            df_gempa = df_asal
        else:
            df_gempa = df_asal[df_asal['Bulan'] == pilihan_bulan]

        gempa_terbaru = df_gempa.iloc[0] if not df_gempa.empty else None

        if 'last_event_id' not in st.session_state:
            st.session_state.last_event_id = df_asal.iloc[0]['EventID']
            st.session_state.is_new_event = False
        
        if df_asal.iloc[0]['EventID'] != st.session_state.last_event_id:
            st.session_state.is_new_event = True
            st.session_state.last_event_id = df_asal.iloc[0]['EventID']
        else:
            st.session_state.is_new_event = False

        info_bgcolor = "rgba(255, 69, 0, 1.0)" if st.session_state.is_new_event else "rgba(255, 255, 255, 0.9)"
        info_bordercolor = "red" if st.session_state.is_new_event else "black"
        judul_info = "🚨 INFO GEMPA TERBARU (BARU) 🚨" if st.session_state.is_new_event else "INFO GEMPA TERBARU"

        peta_warna = {'Dangkal (<= 60 km)': 'red', 'Menengah (61 - 300 km)': 'yellow', 'Dalam (> 300 km)': 'green'}

        fig = px.scatter_mapbox(df_gempa, lat="Lintang", lon="Bujur", size="Magnitudo", color_discrete_sequence=["black"], zoom=6.0, center={"lat": 4.5, "lon": 96.5}, height=650, size_max=22)
        fig.update_traces(hoverinfo='skip', marker=dict(opacity=1.0), showlegend=False)

        df_dirasakan = df_gempa[df_gempa['Dirasakan'].notna()]
        if not df_dirasakan.empty:
            fig.add_trace(go.Scattermapbox(
                lat=df_dirasakan['Lintang'].astype(float).tolist(), 
                lon=df_dirasakan['Bujur'].astype(float).tolist(),
                mode='markers', marker=dict(size=35, color='black', opacity=0.8), 
                name='⚫ Gempa Dirasakan', hoverinfo='skip', showlegend=True
            ))

        fig_fg = px.scatter_mapbox(df_gempa, lat="Lintang", lon="Bujur", size="Magnitudo", color="Kategori Kedalaman", color_discrete_map=peta_warna, hover_name="Kabupaten/Kota", hover_data={"Waktu": True, "Magnitudo": True, "Kedalaman": True, "Kategori Kedalaman": False}, size_max=14)
        for trace in fig_fg.data:
            trace.marker.opacity = 1.0 
            fig.add_trace(trace)

        if gempa_terbaru is not None:
            lat_terbaru = float(gempa_terbaru['Lintang'])
            lon_terbaru = float(gempa_terbaru['Bujur'])
            warna_terbaru = peta_warna.get(gempa_terbaru['Kategori Kedalaman'], 'blue')

            fig.add_trace(go.Scattermapbox(lat=[lat_terbaru], lon=[lon_terbaru], mode='markers', marker=dict(size=45, color='white', opacity=0.9), name='⚪ Gempa Terbaru', hoverinfo='skip', showlegend=True))
            fig.add_trace(go.Scattermapbox(lat=[lat_terbaru], lon=[lon_terbaru], mode='markers', marker=dict(size=14, color=warna_terbaru, opacity=1.0), hoverinfo='skip', showlegend=False))

        fig.add_layout_image(dict(source="https://www.bmkg.go.id/asset/img/logo/logo-bmkg.png", xref="paper", yref="paper", x=0.01, y=0.98, sizex=0.10, sizey=0.10, xanchor="left", yanchor="top"))
        fig.add_annotation(text="<b>Stasiun Geofisika Aceh Besar</b>", xref="paper", yref="paper", x=0.01, y=0.88, showarrow=False, font=dict(size=14, color="black", family="Arial Black"), xanchor="left", yanchor="top")

        if gempa_terbaru is not None:
            info_text = f"<b>{judul_info}</b><br><br><b>Waktu:</b> {gempa_terbaru['Waktu'].strftime('%d %b %Y %H:%M:%S')}<br><b>Mag:</b> {gempa_terbaru['Magnitudo']} | <b>Kedalaman:</b> {gempa_terbaru['Kedalaman']} km<br><b>Lokasi:</b> {gempa_terbaru['Lokasi']}"
            fig.add_annotation(text=info_text, xref="paper", yref="paper", x=0.98, y=0.98, showarrow=False, align="left", bgcolor=info_bgcolor, bordercolor=info_bordercolor, borderwidth=2, borderpad=10, font=dict(size=12, color="black"), xanchor="right", yanchor="top")

        fig.update_layout(
            mapbox_style="carto-positron", 
            mapbox_layers=[{"below": 'traces', "sourcetype": "raster", "source": ["https://server.arcgisonline.com/ArcGIS/rest/services/Ocean/World_Ocean_Base/MapServer/tile/{z}/{y}/{x}"], "sourceattribution": "Tiles © Esri — Sources: GEBCO, NOAA"}],
            margin={"r":0,"t":0,"l":0,"b":0},
            legend=dict(yanchor="bottom", y=0.02, xanchor="left", x=0.02, bgcolor="rgba(255,255,255,0.8)", font=dict(color="black", size=12, family="Arial Black"))
        )

        st.plotly_chart(fig, use_container_width=True, key="peta_aceh_final")

        st.subheader(f"📋 Tabulasi Data Gempa Bumi Periode: {pilihan_bulan}")
        st.dataframe(df_gempa[['Waktu', 'Magnitudo', 'Kedalaman', 'Kabupaten/Kota', 'Lokasi', 'Dirasakan']], use_container_width=True)

        # --- REKAPITULASI ---
        st.divider()
        st.subheader(f"📊 Rekapitulasi Statistik Gempa Bumi ({pilihan_bulan})")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.markdown("**Berdasarkan Magnitudo**")
            st.write(f"• M < 3 (Kecil): **{len(df_gempa[df_gempa['Magnitudo'] < 3])}**")
            st.write(f"• 3 ≤ M < 5 (Sedang): **{len(df_gempa[(df_gempa['Magnitudo'] >= 3) & (df_gempa['Magnitudo'] < 5)])}**")
            st.write(f"• M ≥ 5 (Besar): **{len(df_gempa[df_gempa['Magnitudo'] >= 5])}**")
        with col2:
            st.markdown("**Berdasarkan Kedalaman**")
            st.write(f"• Dangkal (≤ 60 km): **{len(df_gempa[df_gempa['Kedalaman'] <= 60])}**")
            st.write(f"• Menengah (61-300 km): **{len(df_gempa[(df_gempa['Kedalaman'] > 60) & (df_gempa['Kedalaman'] <= 300)])}**")
            st.write(f"• Dalam (> 300 km): **{len(df_gempa[df_gempa['Kedalaman'] > 300])}**")
        with col3:
            st.markdown("**Top 5 Wilayah Teraktif**")
            top_lokasi = df_gempa['Kabupaten/Kota'].value_counts().head(5)
            for lokasi, jumlah in top_lokasi.items():
                st.write(f"• {lokasi}: **{jumlah}**")
        with col4:
            st.markdown("**Berdasarkan Dampak**")
            total = len(df_gempa)
            dirasakan_count = df_gempa['Dirasakan'].notna().sum()
            persen_dirasakan = (dirasakan_count / total * 100) if total > 0 else 0
            st.write(f"• Total Kejadian: **{total}**")
            st.write(f"• Dirasakan: **{dirasakan_count}** ({persen_dirasakan:.1f}%)")
            st.write(f"• Tidak Dirasakan: **{total - dirasakan_count}**")

    # Auto refresh khusus Gempa tiap 30 Detik
    time.sleep(30)
    if hasattr(st, 'rerun'):
        st.rerun()
    else:
        st.experimental_rerun()

elif pilihan_menu == "⚡ Cu চরম (Nowcasting)": # Typo check: it's "Cuaca Ekstrem (Nowcasting)"
# Wait! Fixing the string literal in script.
elif pilihan_menu == "⚡ Cuaca Ekstrem (Nowcasting)":
    st.title("⚡ Monitoring dan Peringatan Dini Cuaca Ekstrem Aceh")

    aceh_map_gdf = load_batas_kecamatan_aceh()

    with st.spinner("Mengunduh data Himawari-9 (Pencarian Otomatis Satelit)..."):
        res = proses_citra_satelit()
        if isinstance(res[0], str):
            error_msg, data_bt = res[0], None
        else:
            data_bt, waktu_data, suhu_min, lons_bahaya, lats_bahaya = res
            error_msg = None

    if data_bt is not None:
        # --- ROW 1: OVERVIEW PROVINSI ---
        col_kiri, col_kanan = st.columns([1.2, 2.8])
        dict_utama_global, dict_meluas_global = None, None

        with col_kiri:
            if st.button("🔄 Refresh Data", use_container_width=True):
                st.cache_data.clear()
                if hasattr(st, 'rerun'):
                    st.rerun()
                else:
                    st.experimental_rerun()
            
            st.metric("🌡️ Suhu Puncak Awan Min.", f"{suhu_min:.1f} °C")
            
            if suhu_min <= THRESHOLD_TEMP:
                dict_utama, dict_meluas, err = get_wilayah_terdampak(lons_bahaya, lats_bahaya, aceh_map_gdf)
                dict_utama_global, dict_meluas_global = dict_utama, dict_meluas
                
                teks_final = format_teks_peringatan(dict_utama, dict_meluas, waktu_data)
                with st.container(height=400): st.error(teks_final)
                st.download_button("📝 Salin Teks Peringatan", teks_final.replace("*", ""), file_name="Warning.txt", use_container_width=True)
            else:
                st.success("✅ **KONDISI AMAN**\n\nSuhu awan saat ini tidak menunjukkan potensi cuaca ekstrem.")

        with col_kanan:
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(11, 8), subplot_kw={'projection': ccrs.PlateCarree()})
            fig.subplots_adjust(wspace=0.1)
            extent_map = [94.5, 98.5, 2.0, 6.0]
            
            # Peta Satelit (Tanpa Watermark)
            ax1.set_extent(extent_map)
            ax1.set_title("Citra Satelit Himawari-9", fontsize=10, fontweight='bold')
            im = ax1.imshow(data_bt, origin='upper', extent=extent_map, cmap=cmap_bmkg, norm=norm_bmkg)
            if aceh_map_gdf is not None: aceh_map_gdf.boundary.plot(ax=ax1, color='black', linewidth=0.1)
            ax1.coastlines(color='white', linewidth=0.5)
            plt.colorbar(im, ax=ax1, orientation='horizontal', pad=0.05, label="Suhu (°C)")
            
            # Peta Klasifikasi Provinsi (Tanpa Watermark)
            ax2.set_extent(extent_map)
            ax2.set_title("Peta Klasifikasi Risiko Wilayah", fontsize=10, fontweight='bold')
            
            if aceh_map_gdf is not None:
                df_prov = aceh_map_gdf.copy()
                list_merah = [k for kecs in (dict_utama_global.values() if dict_utama_global else []) for k in kecs]
                list_kuning = [k for kecs in (dict_meluas_global.values() if dict_meluas_global else []) for k in kecs]
                
                def set_color(x):
                    if x in list_merah: return COLOR_EKSTREM
                    if x in list_kuning: return COLOR_POTENSI
                    return COLOR_AMAN
                
                df_prov.plot(ax=ax2, color=df_prov[KOLOM_KECAMATAN].apply(set_color), edgecolor='black', linewidth=0.1)
                
                p1 = mpatches.Patch(color=COLOR_EKSTREM, label='🔴 Terdampak Saat Ini')
                p2 = mpatches.Patch(color=COLOR_POTENSI, label='🟡 Potensi Meluas Ke')
                p3 = mpatches.Patch(color=COLOR_AMAN, label='⚪ Aman')
                ax2.legend(handles=[p1, p2, p3], loc='lower center', bbox_to_anchor=(0.5, -0.2), ncol=1, frameon=False, fontsize=9)
            
            st.pyplot(fig, use_container_width=True)

        # --- ROW 2: DETAIL PER KABUPATEN ---
        if dict_utama_global:
            st.divider()
            st.subheader("📍 Detail Spasial Per Kabupaten")
            kab_terdampak = sorted(list(dict_utama_global.keys()))
            tabs = st.tabs(kab_terdampak)
            
            for i, nama_kab in enumerate(kab_terdampak):
                with tabs[i]:
                    c1, c2 = st.columns([1, 2])
                    gdf_kab = aceh_map_gdf[aceh_map_gdf[KOLOM_KABUPATEN] == nama_kab].copy()
                    kec_m = dict_utama_global.get(nama_kab, [])
                    kec_k = dict_meluas_global.get(nama_kab, []) if dict_meluas_global else []
                    
                    with c1:
                        st.write(f"### {nama_kab}")
                        if kec_m: st.error(f"**Terdampak (Merah):**\n" + ", ".join(kec_m))
                        if kec_k: st.warning(f"**Meluas (Kuning):**\n" + ", ".join(kec_k))
                    
                    with c2:
                        fig_k, ax_k = plt.subplots(figsize=(8, 6))
                        fig_k.patch.set_edgecolor('black')
                        fig_k.patch.set_linewidth(1) # Frame Peta
                        
                        def color_k_update(row):
                            if row[KOLOM_KECAMATAN] in kec_m: return COLOR_EKSTREM
                            if row[KOLOM_KECAMATAN] in kec_k: return COLOR_POTENSI
                            return COLOR_AMAN
                        
                        # Plot wilayah kabupaten dengan zorder standar
                        gdf_kab.plot(ax=ax_k, color=gdf_kab.apply(color_k_update, axis=1), edgecolor='black', linewidth=0.5)
                        for x, y, label in zip(gdf_kab.geometry.centroid.x, gdf_kab.geometry.centroid.y, gdf_kab[KOLOM_KECAMATAN]):
                            ax_k.text(x, y, label, fontsize=4, ha='center', fontweight='bold', alpha=0.7)
                        
                        ax_k.axis('off')

                        # Legenda
                        p1_tab = mpatches.Patch(color=COLOR_EKSTREM, label='🔴 Terdampak Saat Ini')
                        p2_tab = mpatches.Patch(color=COLOR_POTENSI, label='🟡 Potensi Meluas Ke')
                        p3_tab = mpatches.Patch(color=COLOR_AMAN, label='⚪ Aman')
                        ax_k.legend(handles=[p1_tab, p2_tab, p3_tab], loc='lower center', bbox_to_anchor=(0.5, -0.15), ncol=1, frameon=False, fontsize=8)

                        # WATERMARK DI TENGAH PETA DETAIL
                        add_central_watermark(ax_k, NAMA_FILE_WATERMARK, alpha=0.1, zoom=0.1)

                        # Arah Mata Angin
                        add_north_arrow_to_fig(fig_k, ax_k, location=(0.90, 0.90), size=0.1, color='black')

                        st.pyplot(fig_k)
    else:
        st.error(f"Gagal memuat citra. Error: {error_msg}")

    # Auto refresh khusus Cuaca tiap 10 Menit 
    time.sleep(600)
    if hasattr(st, 'rerun'):
        st.rerun()
    else:
        st.experimental_rerun()
