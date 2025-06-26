#app.py

import streamlit as st
import redis
import pandas as pd
import json
import os
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh

# --- Configurações do Redis ---
REDIS_HOST = os.environ.get("REDIS_HOST", "redis")
REDIS_PORT = os.environ.get("REDIS_PORT", "6379")

# --- Configurações da Página ---
# Define o título, ícone, layout e estado inicial da barra lateral do aplicativo Streamlit.
st.set_page_config(
    page_title="Dashboard de E-commerce | Live + Histórico",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Estilos CSS Personalizados ---
# Injeta CSS para criar cartões de métricas visualmente atraentes e estilizar outros elementos da interface.
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
    height: 100%; /* Garante que os cartões na mesma linha tenham a mesma altura */
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
    color: #2E8B57; /* Verde para valores positivos */
}
.conversion-card-value {
    font-size: 28px;
    font-weight: bold;
    color: #333333;
    margin: 5px 0;
}
.abandon-value {
    color: #DC143C; /* Vermelho para valores de abandono */
}
.conversion-value {
    color: #2E8B57; /* Verde para valores de conversão */
}
</style>
""", unsafe_allow_html=True)

# --- Conexão com Redis ---
@st.cache_resource
def get_redis_connection():
    """
    Estabelece e armazena em cache a conexão com o Redis.
    Retorna o objeto de conexão ou None em caso de falha.
    """
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        r.ping()
        print(f"Conexão com Redis ({REDIS_HOST}) estabelecida/reutilizada.")
        return r
    except redis.exceptions.ConnectionError as e:
        st.error(f"Não foi possível conectar ao Redis em '{REDIS_HOST}'. Detalhes: {e}")
        return None

# --- Funções de Busca de Dados ---
def fetch_redis_data(redis_conn, key):
    """
    Busca um dado do Redis pela chave e o desserializa de JSON.
    Retorna o dado (dict/list) ou None se a chave não existir ou houver erro.
    """
    if not redis_conn: return None
    data = redis_conn.get(key)
    if data:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            # Lida com o caso de o dado não ser um JSON válido
            return None
    return None

def get_realtime_kpis(r):
    """
    Busca as métricas globais em tempo real (Receita, Pedidos, Ticket Médio).
    Lida com possíveis inconsistências no formato dos dados do Redis (lista vs. dict).
    """
    metrics_data = fetch_redis_data(r, "realtime:metricas_globais")
    metrics = {} # Dicionário padrão

    if metrics_data:
        # Normaliza o dado para sempre ser um dicionário
        if isinstance(metrics_data, list) and len(metrics_data) > 0:
            metrics = metrics_data[0]
        elif isinstance(metrics_data, dict):
            metrics = metrics_data

    # Retorna os KPIs com valores padrão de 0 caso não sejam encontrados
    return {
        "receita": metrics.get("receita_total_global", 0),
        "pedidos": metrics.get("pedidos_totais_global", 0),
        "ticket_medio": metrics.get("ticket_medio_global", 0)
    }

def get_conversion_rates(r):
    """
    Busca e calcula as taxas de conversão e abandono de carrinho.
    """
    carrinhos_criados_data = fetch_redis_data(r, "realtime:total_carrinhos_criados")
    carrinhos_convertidos_data = fetch_redis_data(r, "realtime:total_carrinhos_convertidos")

    def get_total_from_data(data):
        """Função auxiliar para extrair o valor 'total' de forma segura."""
        if not data: return 0
        item = data[0] if isinstance(data, list) and len(data) > 0 else data
        return item.get('total', 0) if isinstance(item, dict) else 0

    criados = get_total_from_data(carrinhos_criados_data)
    convertidos = get_total_from_data(carrinhos_convertidos_data)
    
    taxa_conversao = (convertidos / criados * 100) if criados > 0 else 0
    taxa_abandono = 100 - taxa_conversao if criados > 0 else 0
    
    return {
        "carrinhos_criados": criados,
        "carrinhos_convertidos": convertidos,
        "taxa_conversao": taxa_conversao,
        "taxa_abandono": taxa_abandono
    }

# --- Inicialização e Auto-Refresh ---
r = get_redis_connection()
# Recarrega a página a cada 5 segundos para buscar novos dados
st_autorefresh(interval=5000, key="data_refresher")

# Busca os dados atuais
current_kpis = get_realtime_kpis(r)
current_rates = get_conversion_rates(r)
last_update_time = datetime.now().strftime('%H:%M:%S')

# Inicializa o estado da sessão para comparação e notificações
if 'previous_kpis' not in st.session_state:
    st.session_state['previous_kpis'] = current_kpis

# Notificação inteligente para o usuário
if current_kpis != st.session_state['previous_kpis']:
    st.toast('Novos dados chegaram!')
    st.session_state['previous_kpis'] = current_kpis
else:
    st.toast(f'Verificado às {last_update_time}. Sem novos dados.')

# --- Barra Lateral (Sidebar) ---
with st.sidebar:
    st.title("E-commerce Live View")
    st.markdown("---")
    st.markdown(f"**Última Verificação:** `{last_update_time}`")
    st.info("Este dashboard exibe métricas em tempo real e históricas de uma plataforma de e-commerce.")

# --- Interface Principal do Dashboard ---
st.title("Dashboard Completo de E-commerce")

if not r:
    st.warning("Aguardando conexão com as fontes de dados...")
    st.stop() # Interrompe a execução se não houver conexão com o Redis

# Seção 1: Métricas em Tempo Real
st.markdown("## Métricas em Tempo Real")
col1, col2, col3 = st.columns(3)
with col1:
    st.markdown(f'<div class="metric-card"><div class="metric-title">💰 Receita Total</div><div class="metric-value">R$ {current_kpis["receita"]:,.2f}</div></div>', unsafe_allow_html=True)
with col2:
    st.markdown(f'<div class="metric-card"><div class="metric-title">📦 Pedidos Totais</div><div class="metric-value">{current_kpis["pedidos"]:,}</div></div>', unsafe_allow_html=True)
with col3:
    st.markdown(f'<div class="metric-card"><div class="metric-title">🏷️ Ticket Médio</div><div class="metric-value">R$ {current_kpis["ticket_medio"]:,.2f}</div></div>', unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

# Funil de Conversão
st.subheader("Análise do Funil de Conversão")
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

# Gráficos em Tempo Real
st.markdown("---")
st.subheader("Visualizações em Tempo Real")
col1, col2 = st.columns([6, 4]) # Colunas com proporções diferentes

with col1:
    receita_categoria_data = fetch_redis_data(r, "realtime:receita_por_categoria")
    if receita_categoria_data:
        df_cat = pd.DataFrame(receita_categoria_data).sort_values("receita", ascending=False)
        fig_bar = px.bar(df_cat, x="receita", y="categoria", orientation='h',
                         text='receita', template="seaborn",
                         labels={"receita": "Receita (R$)", "categoria": "Categoria"},
                         title="Receita por Categoria")
        fig_bar.update_traces(texttemplate='R$ %{text:,.2f}', textposition='outside', marker_color='#4682B4')
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("Aguardando dados de receita por categoria...")

with col2:
    top_produtos_data = fetch_redis_data(r, "realtime:top_5_produtos")
    if top_produtos_data:
        df_top = pd.DataFrame(top_produtos_data).rename(columns={"item": "Produto", "total_vendido": "Qtd. Vendida"})
        st.dataframe(df_top.style.format({"Qtd. Vendida": "{:,}"})
                      .background_gradient(cmap='Greens', subset=['Qtd. Vendida'])
                      .set_caption("Top 5 Produtos Mais Vendidos"),
                      use_container_width=True)
    else:
        st.info("Aguardando dados de produtos mais vendidos...")

# Seção 2: Análise Histórica
st.markdown("---")
st.markdown("## Análise Histórica")

# Abas para organizar as análises históricas
tab1, tab2, tab3 = st.tabs(["Crescimento de Receita Diário", "Top Produtos por Trimestre", "Taxa de Conversão Histórica"])

with tab1:
    st.subheader("Evolução da Receita Diária por Segmento de Cliente")
    df_crescimento_data = fetch_redis_data(r, "historical:daily_revenue_metrics")
    # df_crescimento_data = json.loads(df_crescimento_data_str)
    
    if df_crescimento_data:
        df_crescimento = pd.DataFrame(df_crescimento_data)
        # Converte a coluna de data para o formato datetime, essencial para gráficos de séries temporais
        df_crescimento['data'] = pd.to_datetime(df_crescimento['data'])
        df_crescimento = df_crescimento.sort_values('data')

        # Cria o gráfico de linhas com Plotly Express
        fig_line = px.line(df_crescimento,
                           x='data',
                           y='receita_total_diaria',
                           color='segmento_cliente', # Cria uma linha para cada segmento
                           title="Receita Diária por Segmento de Cliente",
                           labels={'data': 'Data', 'receita_total_diaria': 'Receita Total (R$)', 'segmento_cliente': 'Segmento'},
                           markers=True) # Adiciona marcadores para cada ponto de dado
        
        fig_line.update_layout(legend_title_text='Segmento')
        st.plotly_chart(fig_line, use_container_width=True)
    else:
        st.info("Aguardando dados históricos de crescimento de receita...")

with tab2:
    st.subheader("Top 10 Produtos Mais Vendidos por Período")
    df_top_prod_data = fetch_redis_data(r, "historical:top_products_quarterly")
    # df_top_prod_data = json.loads(df_top_prod_data_str)

    if df_top_prod_data:
        df_top_prod = pd.DataFrame(df_top_prod_data)
        
        # Cria filtros para ano e trimestre
        col_filter1, col_filter2 = st.columns(2)
        anos = sorted(df_top_prod['ano'].unique())
        ano_selecionado = col_filter1.selectbox("Selecione o Ano", options=anos, index=len(anos)-1)
        
        trimestres = sorted(df_top_prod[df_top_prod['ano'] == ano_selecionado]['trimestre'].unique())
        trimestre_selecionado = col_filter2.selectbox("Selecione o Trimestre", options=trimestres, index=len(trimestres)-1)

        # Filtra o DataFrame com base na seleção do usuário
        df_filtrado = df_top_prod[(df_top_prod['ano'] == ano_selecionado) & (df_top_prod['trimestre'] == trimestre_selecionado)]

        # Cria o gráfico de barras horizontais
        fig_bar_prod = px.bar(df_filtrado,
                              x='unidades_vendidas',
                              y='nome_produto',
                              orientation='h',
                              title=f"Top Produtos - {trimestre_selecionado}º Tri de {ano_selecionado}",
                              labels={'unidades_vendidas': 'Unidades Vendidas', 'nome_produto': 'Produto'},
                              text='unidades_vendidas',
                              template='plotly_white')
        
        fig_bar_prod.update_layout(yaxis={'categoryorder':'total ascending'}) # Ordena do menor para o maior
        fig_bar_prod.update_traces(textposition='outside', marker_color='#2E8B57')
        st.plotly_chart(fig_bar_prod, use_container_width=True)

    else:
        st.info("Aguardando dados históricos de top produtos...")

with tab3:
    st.subheader("Taxa de Abandono de Carrinho (Histórico Global)")
    # Corrigindo o nome da chave para buscar o dado correto
    taxa_abandono_str = r.get("historical:abandoned_cart_rate")  # Usando r.get() direto aqui

    if taxa_abandono_str:
        try:
            # O dado no Redis é algo como '{"value":"70.50"}'
            # Precisamos extrair esse valor
            # if isinstance(taxa_abandono_str, bytes):
            #     taxa_abandono_str = taxa_abandono_str.decode("utf-8")
            dados_abandono = json.loads(taxa_abandono_str)
            taxa_abandono_hist = float(dados_abandono.get("value", 0))

            # Cria o gráfico de medidor para a TAXA DE ABANDONO
            fig_gauge = go.Figure(go.Indicator(
                mode="gauge+number",
                value=taxa_abandono_hist,
                title={'text': "Taxa de Abandono Média (%)"},
                gauge={
                    'axis': {'range': [None, 100], 'tickwidth': 1, 'tickcolor': "darkblue"},
                    'bar': {'color': "#2E8B57"},
                    'bgcolor': "white",
                    'borderwidth': 2,
                    'bordercolor': "gray",
                    'steps': [
                        {'range': [0, 25], 'color': '#FF7F7F'},
                        {'range': [25, 50], 'color': '#FFD700'}
                    ],
                    'threshold': {
                        'line': {'color': "red", 'width': 4},
                        'thickness': 0.75,
                        'value': 30  # Exemplo de meta
                    }
                }
            ))
            # fig_gauge.update_layout(font = {'color': "darkblue", 'family': "Arial"})
            st.plotly_chart(fig_gauge, use_container_width=True)
        except (json.JSONDecodeError, ValueError, TypeError) as e:
            st.error(f"Erro ao processar a taxa de abandono: {e}")
            st.info(f"Dado recebido do Redis: {taxa_abandono_str}")
    else:
        st.info("Aguardando dados históricos da taxa de conversão...")

