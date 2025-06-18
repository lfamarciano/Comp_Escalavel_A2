import streamlit as st
import redis
import pandas as pd
import json
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

st.set_page_config(
    page_title="Dashboard de E-commerce | Live",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.markdown("""
<style>
.metric-card {
    background-color: #FFFFFF;
    border-radius: 10px;
    box-shadow: 0 4px 8px 0 rgba(0,0,0,0.2);
    transition: 0.3s;
    padding: 20px;
    margin: 10px;
    text-align: center;
    height: 100%; /* Garante que os cards na mesma linha tenham a mesma altura */
}
.metric-card:hover {
    box-shadow: 0 8px 16px 0 rgba(0,0,0,0.2);
}
.metric-title {
    font-size: 18px;
    font-weight: bold;
    color: #4F4F4F;
}
.metric-value {
    font-size: 36px;
    font-weight: bold;
    color: #2E8B57;
}
.conversion-card-value {
    font-size: 28px;
    font-weight: bold;
    color: #333333;
    margin: 5px 0;
}
.abandon-value {
    color: #DC143C; /* Vermelho Crimson */
}
.conversion-value {
    color: #2E8B57; /* Verde Mar */
}
</style>
""", unsafe_allow_html=True)


# --- Conexão com o Redis ---
@st.cache_resource
def get_redis_connection():
    try:
        r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
        r.ping()
        print("Conexão com Redis estabelecida/reutilizada.")
        return r
    except redis.exceptions.ConnectionError as e:
        st.error(f"Não foi possível conectar ao Redis. Verifique se o container Docker está em execução. Detalhes: {e}")
        return None

# --- Funções de busca de dados (sem cache para dados em tempo real) ---
def fetch_redis_data(redis_conn, key):
    if not redis_conn: return None
    data = redis_conn.get(key)
    if data:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return None
    return None

def get_realtime_kpis(r):
    metrics = fetch_redis_data(r, "realtime:metricas_globais")
    if metrics:
        return {
            "receita": metrics.get("receita_total_global", 0),
            "pedidos": metrics.get("pedidos_totais_global", 0),
            "ticket_medio": metrics.get("ticket_medio_global", 0)
        }
    return {"receita": 0, "pedidos": 0, "ticket_medio": 0}

def get_conversion_rates(r):
    carrinhos_criados_data = fetch_redis_data(r, "realtime:total_carrinhos_criados")
    carrinhos_convertidos_data = fetch_redis_data(r, "realtime:total_carrinhos_convertidos")
    criados = carrinhos_criados_data.get('total', 0) if carrinhos_criados_data else 0
    convertidos = carrinhos_convertidos_data.get('total', 0) if carrinhos_convertidos_data else 0
    taxa_conversao = (convertidos / criados * 100) if criados > 0 else 0
    taxa_abandono = 100 - taxa_conversao if criados > 0 else 0
    return {
        "carrinhos_criados": criados,
        "carrinhos_convertidos": convertidos,
        "taxa_conversao": taxa_conversao,
        "taxa_abandono": taxa_abandono
    }

# --- Inicialização e Lógica de Notificação ---
r = get_redis_connection()
st_autorefresh(interval=5000, key="data_refresher")

# Busca os dados atuais
current_kpis = get_realtime_kpis(r)
current_rates = get_conversion_rates(r)
last_update_time = datetime.now().strftime('%H:%M:%S')

# Inicializa o estado da sessão para comparação
if 'previous_kpis' not in st.session_state:
    st.session_state['previous_kpis'] = current_kpis

# Compara dados atuais com os anteriores para a notificação inteligente
if current_kpis != st.session_state['previous_kpis']:
    st.toast('🚀 Novos dados chegaram!', icon='🚀')
    st.session_state['previous_kpis'] = current_kpis  # Atualiza o estado
else:
    st.toast(f'Verificado às {last_update_time}. Sem novos dados.', icon='🔄')

# --- Barra Lateral (Sidebar) ---
with st.sidebar:
    st.image("dash_image_old_pc.png", width=150)
    st.title("E-commerce Live View")
    st.markdown("---")
    st.markdown(f"**Última Verificação:** `{last_update_time}`")
    st.info("Este dashboard exibe métricas em tempo real de uma plataforma de e-commerce.")

# --- Interface Principal do Dashboard ---
st.title("🛒 Dashboard de Métricas de E-commerce")
if not r:
    st.warning("Aguardando conexão com a fonte de dados (Redis)...")
    st.stop()

st.markdown("## 📈 Métricas em Tempo Real")
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f'<div class="metric-card"><div class="metric-title">💰 Receita Total</div><div class="metric-value">R$ {current_kpis["receita"]:,.2f}</div></div>', unsafe_allow_html=True)
with col2:
    st.markdown(f'<div class="metric-card"><div class="metric-title">📦 Pedidos Totais</div><div class="metric-value">{current_kpis["pedidos"]:,}</div></div>', unsafe_allow_html=True)
with col3:
    st.markdown(f'<div class="metric-card"><div class="metric-title">🏷️ Ticket Médio</div><div class="metric-value">R$ {current_kpis["ticket_medio"]:,.2f}</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

st.subheader("🛒 Análise do Funil de Conversão")
col1, col2, col3 = st.columns(3)

with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">Etapas do Funil</div>
        <p class="conversion-card-value"><strong>{current_rates['carrinhos_criados']:,}</strong> Carrinhos Criados</p>
        <p class="conversion-card-value"><strong>{current_rates['carrinhos_convertidos']:,}</strong> Compras Concluídas</p>
    </div>
    """, unsafe_allow_html=True)

with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">✅ Taxa de Conversão</div>
        <p class="metric-value conversion-value">{current_rates['taxa_conversao']:.2f}%</p>
    </div>
    """, unsafe_allow_html=True)

with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-title">❌ Taxa de Abandono</div>
        <p class="metric-value abandon-value">{current_rates['taxa_abandono']:.2f}%</p>
    </div>
    """, unsafe_allow_html=True)

st.markdown("---")

# --- Gráficos em Tempo Real ---
receita_categoria_data = fetch_redis_data(r, "realtime:receita_por_categoria")
top_produtos_data = fetch_redis_data(r, "realtime:top_5_produtos")
col1, col2 = st.columns([6, 4])
with col1:
    st.subheader("💰 Receita por Categoria")
    if receita_categoria_data:
        df_cat = pd.DataFrame(receita_categoria_data).sort_values("receita", ascending=False)
        fig_bar = px.bar(df_cat, x="receita", y="categoria", orientation='h', text='receita', template="seaborn", labels={"receita": "Receita (R$)", "categoria": "Categoria"})
        fig_bar.update_traces(texttemplate='R$ %{text:,.2f}', textposition='outside', marker_color='#4682B4')
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("Aguardando dados de receita por categoria...")
with col2:
    st.subheader("🔥 Top 5 Produtos Mais Vendidos")
    if top_produtos_data:
        df_top = pd.DataFrame(top_produtos_data).rename(columns={"item": "Produto", "total_vendido": "Qtd. Vendida"})
        st.dataframe(df_top.style.format({"Qtd. Vendida": "{:,}"}).background_gradient(cmap='Greens', subset=['Qtd. Vendida']), use_container_width=True)
    else:
        st.info("Aguardando dados de produtos mais vendidos...")

# --- Seção de Análise Histórica ---
st.markdown("<br><hr><br>", unsafe_allow_html=True)
st.markdown("## ⏳ Análise Histórica")
dados_historicos = fetch_redis_data(r, "historical:daily_revenue_metrics")
if dados_historicos:
    df_hist = pd.DataFrame(dados_historicos)
    df_hist['data'] = pd.to_datetime(df_hist['data'])
    df_hist = df_hist.sort_values('data')
    st.subheader("📅 Crescimento da Receita Diária por Segmento")
    segmentos = df_hist['segmento_cliente'].unique()
    segmento_selecionado = st.multiselect('Selecione para visualizar:', options=segmentos, default=segmentos)
    df_filtrado = df_hist[df_hist['segmento_cliente'].isin(segmento_selecionado)]
    fig_hist = px.line(df_filtrado, x='data', y='receita_total_diaria', color='segmento_cliente', title='Receita Total Diária ao Longo do Tempo', labels={"data": "Data", "receita_total_diaria": "Receita Diária (R$)", "segmento_cliente": "Segmento"}, template="plotly_white", markers=True)
    fig_hist.update_layout(legend_title_text='Segmentos')
    st.plotly_chart(fig_hist, use_container_width=True)
    with st.expander("Ver dados históricos detalhados"):
        st.dataframe(df_filtrado)
else:
    st.info("Aguardando dados históricos. Execute o pipeline `publish_historical_to_redis.py`.")