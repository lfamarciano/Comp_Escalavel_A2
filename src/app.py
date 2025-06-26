import streamlit as st
import redis
import pandas as pd
import json
import os
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
from streamlit_autorefresh import st_autorefresh
import psycopg2

import sys
from pathlib import Path
sys.path.append(str(Path(__file__).parent.parent))

from config import (
    POSTGRES_PASSWORD,
    POSTGRES_DATABASE,
    POSTGRES_USER,
    POSTGRES_HOST,
    POSTGRES_PORT,
    REDIS_HOST,
    REDIS_PORT
)


# Configurações da página
st.set_page_config(
    page_title="Dashboard de E-commerce | Live + Histórico",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Estilos CSS personalizados
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
    height: 100%;
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
    color: #DC143C;
}
.conversion-value {
    color: #2E8B57;
}
</style>
""", unsafe_allow_html=True)

# --- Conexões com Bancos de Dados ---
@st.cache_resource
def get_redis_connection():
    try:
        r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True, ssl=True, ssl_cert_reqs=None)
        r.ping()
        print(f"Conexão com Redis ({REDIS_HOST}) estabelecida/reutilizada.")
        return r
    except redis.exceptions.ConnectionError as e:
        st.error(f"Não foi possível conectar ao Redis em '{REDIS_HOST}'. Detalhes: {e}")
        return None

@st.cache_resource
def get_postgres_connection():
    try:
        # CORRIGIDO: Usa as variáveis de ambiente para a conexão
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            database=POSTGRES_DATABASE,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            port=POSTGRES_PORT
        )
        print(f"Conexão com PostgreSQL ({POSTGRES_HOST}) estabelecida/reutilizada.")
        return conn
    except Exception as e:
        st.error(f"Erro ao conectar ao PostgreSQL em '{POSTGRES_HOST}': {e}")
        return None

# --- Funções de busca de dados ---
def fetch_redis_data(redis_conn, key):
    if not redis_conn: return None
    data = redis_conn.get(key)
    if data:
        try:
            return json.loads(data)
        except json.JSONDecodeError:
            return None
    return None

def query_postgres(query, conn):
    try:
        return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Erro na consulta SQL: {e}")
        return pd.DataFrame()

def get_realtime_kpis(r):
    """Busca as métricas globais e lida com o formato de lista ou dicionário."""
    metrics_data = fetch_redis_data(r, "realtime:metricas_globais")
    metrics = {} # Inicia com um dicionário vazio por segurança

    if metrics_data:
        # Se o dado do Redis for uma lista (devido ao bug de formato),
        # pegamos o primeiro (e único) dicionário de dentro dela.
        if isinstance(metrics_data, list) and len(metrics_data) > 0:
            metrics = metrics_data[0]
        # Se já estiver no formato correto de dicionário, apenas usamos.
        elif isinstance(metrics_data, dict):
            metrics = metrics_data

    return {
        "receita": metrics.get("receita_total_global", 0),
        "pedidos": metrics.get("pedidos_totais_global", 0),
        "ticket_medio": metrics.get("ticket_medio_global", 0)
    }

def get_conversion_rates(r):
    """Busca os dados de conversão e lida com o formato de lista ou dicionário."""
    carrinhos_criados_data = fetch_redis_data(r, "realtime:total_carrinhos_criados")
    carrinhos_convertidos_data = fetch_redis_data(r, "realtime:total_carrinhos_convertidos")

    # Função auxiliar para extrair o total de forma segura
    def get_total_from_data(data):
        if not data:
            return 0
        
        # Se for uma lista, pega o primeiro elemento
        if isinstance(data, list) and len(data) > 0:
            item = data[0]
        else:
            item = data

        # Garante que o item é um dicionário antes de usar .get()
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

# --- Inicialização ---
r = get_redis_connection()
pg_conn = get_postgres_connection()
st_autorefresh(interval=5000, key="data_refresher")

# Busca os dados atuais
current_kpis = get_realtime_kpis(r)
current_rates = get_conversion_rates(r)
last_update_time = datetime.now().strftime('%H:%M:%S')

# Inicializa o estado da sessão para comparação
if 'previous_kpis' not in st.session_state:
    st.session_state['previous_kpis'] = current_kpis

# Notificação inteligente
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

if not r or not pg_conn:
    st.warning("Aguardando conexão com as fontes de dados...")
    st.stop()

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
col1, col2 = st.columns([6, 4])

with col1:
    receita_categoria_data = fetch_redis_data(r, "realtime:receita_por_categoria")
    if receita_categoria_data:
        df_cat = pd.DataFrame(receita_categoria_data).sort_values("receita", ascending=False)
        fig_bar = px.bar(df_cat, x="receita", y="categoria", orientation='h', 
                        text='receita', template="seaborn", 
                        labels={"receita": "Receita (R$)", "categoria": "Categoria"})
        fig_bar.update_traces(texttemplate='R$ %{text:,.2f}', textposition='outside', marker_color='#4682B4')
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'}, title="Receita por Categoria")
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
tab1, tab2, tab3, tab4, tab5 = st.tabs(["Crescimento Mensal", "Sazonalidade", "Abandono", "LTV", "Sazonalidade2"])

with tab1:
    st.subheader("Crescimento da Receita Mês a Mês")
    query_crescimento = """
        SELECT 
            DATE_TRUNC('month', data_compra) as mes,
            SUM(valor_total_compra) as receita,
            LAG(SUM(valor_total_compra), 1) OVER (ORDER BY DATE_TRUNC('month', data_compra)) as receita_anterior,
            (SUM(valor_total_compra) - LAG(SUM(valor_total_compra), 1) OVER (ORDER BY DATE_TRUNC('month', data_compra))) / 
     LAG(SUM(valor_total_compra), 1) OVER (ORDER BY DATE_TRUNC('month', data_compra)) * 100 as crescimento
        FROM transacoes_vendas
        GROUP BY mes
        ORDER BY mes DESC
        LIMIT 12
    """
    df_crescimento = query_postgres(query_crescimento, pg_conn)
    
    if not df_crescimento.empty:
        fig = px.line(df_crescimento, x='mes', y='crescimento',
                     title='Crescimento Percentual Mês a Mês',
                     labels={'mes': 'Mês', 'crescimento': 'Crescimento (%)'})
        fig.add_hline(y=0, line_dash="dash", line_color="red")
        st.plotly_chart(fig, use_container_width=True)
        
        # Detalhes em tabela
        st.dataframe(df_crescimento.style.format({
            'mes': lambda x: x.strftime('%Y-%m'),
            'receita': "R${:,.2f}",
            'receita_anterior': "R${:,.2f}",
            'crescimento': "{:.1f}%"
        }))
    else:
        st.warning("Nenhum dado disponível para análise de crescimento")

with tab2:
    st.subheader("Produtos Mais Vendidos")
    query_sazonal = """
        WITH vendas_trimestrais AS (
            SELECT 
                p.nome_produto,
                DATE_TRUNC('quarter', t.data_compra) as trimestre,
                COUNT(*) as vendas,
                ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('quarter', t.data_compra) ORDER BY COUNT(*) DESC) as rank
            FROM transacoes_vendas t
            JOIN catalogo_produtos p ON t.id_produto = p.id_produto
            WHERE t.data_compra >= NOW() - INTERVAL '1 year'
            GROUP BY p.nome_produto, trimestre
        )
        SELECT trimestre, nome_produto, vendas
        FROM vendas_trimestrais
        WHERE rank <= 10
        ORDER BY trimestre DESC, vendas DESC
    """
    df_sazonal = query_postgres(query_sazonal, pg_conn)

    if not df_sazonal.empty:
        df_sazonal['trimestre'] = df_sazonal['trimestre'].dt.strftime('%Y-Q%q')
        fig = px.bar(df_sazonal, x='trimestre', y='vendas', color='nome_produto',
                    title='Top 10 Produtos por Trimestre',
                    labels={'trimestre': 'Trimestre', 'vendas': 'Vendas', 'nome_produto': 'Produto'})
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("Nenhum dado disponível para análise sazonal")

with tab3:
    st.subheader("Taxa de Abandono de Carrinho")
    query_abandono = """
        WITH carrinhos_criados AS (
            SELECT DISTINCT id_carrinho 
            FROM eventos_web 
            WHERE tipo_evento = 'carrinho_criado' AND id_carrinho IS NOT NULL
        ),
        carrinhos_convertidos AS (
            SELECT DISTINCT id_carrinho 
            FROM transacoes_vendas 
            WHERE id_carrinho IS NOT NULL
        )
        SELECT 
            (SELECT COUNT(*) FROM carrinhos_criados) as total_carrinhos,
            (SELECT COUNT(*) FROM carrinhos_criados cc 
             WHERE NOT EXISTS (
                 SELECT 1 FROM carrinhos_convertidos cv WHERE cv.id_carrinho = cc.id_carrinho
             )) as carrinhos_abandonados,
            ((SELECT COUNT(*) FROM carrinhos_criados cc 
              WHERE NOT EXISTS (
                  SELECT 1 FROM carrinhos_convertidos cv WHERE cv.id_carrinho = cc.id_carrinho
              )) * 100.0 / NULLIF((SELECT COUNT(*) FROM carrinhos_criados), 0)) as taxa_abandono
    """
    df_abandono = query_postgres(query_abandono, pg_conn)

    if not df_abandono.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Carrinhos Criados (Histórico)", f"{df_abandono.iloc[0]['total_carrinhos']:,}")
        with col2:
            st.metric("Taxa de Abandono (Histórico)", f"{df_abandono.iloc[0]['taxa_abandono']:.1f}%")
        
        # Comparação com taxa em tempo real
        st.markdown("**Comparação com Taxa em Tempo Real**")
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Taxa Atual", f"{current_rates['taxa_abandono']:.1f}%")
        with col2:
            diff = current_rates['taxa_abandono'] - df_abandono.iloc[0]['taxa_abandono']
            st.metric("Diferença", f"{diff:.1f}%", delta=f"{diff:.1f}%")
    else:
        st.warning("Nenhum dado disponível para cálculo de abandono histórico")

with tab4:
    st.subheader("Valor do Tempo de Vida do Cliente (LTV) por Segmento")
    query_ltv = """
        SELECT 
            dc.segmento_cliente,
            COUNT(DISTINCT dc.id_usuario) as total_clientes,
            SUM(tv.valor_total_compra) as receita_total,
            SUM(tv.valor_total_compra) / NULLIF(COUNT(DISTINCT dc.id_usuario), 0) as ltv
        FROM dados_clientes dc
        LEFT JOIN transacoes_vendas tv ON dc.id_usuario = tv.id_usuario
        GROUP BY dc.segmento_cliente
        ORDER BY ltv DESC
    """
    df_ltv = query_postgres(query_ltv, pg_conn)

    if not df_ltv.empty:
        fig = px.bar(df_ltv, x='segmento_cliente', y='ltv',
                    title='Valor do Tempo de Vida (LTV) por Segmento',
                    labels={'segmento_cliente': 'Segmento', 'ltv': 'LTV (R$)'})
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(df_ltv.style.format({
            'total_clientes': "{:,}",
            'receita_total': "R${:,.2f}",
            'ltv': "R${:,.2f}"
        }))
    else:
        st.warning("Nenhum dado disponível para cálculo de LTV")

with tab5:
    st.subheader("Análise Sazonal de Produtos")
    
    query_sazonal = """
        WITH vendas_mensais AS (
            SELECT 
                p.nome_produto,
                DATE_TRUNC('month', t.data_compra) as mes,
                COUNT(*) as vendas,
                ROW_NUMBER() OVER (PARTITION BY DATE_TRUNC('month', t.data_compra) ORDER BY COUNT(*) DESC) as rank
            FROM transacoes_vendas t
            JOIN catalogo_produtos p ON t.id_produto = p.id_produto
            WHERE t.data_compra >= NOW() - INTERVAL '2 years'
            GROUP BY p.nome_produto, mes
        )
        SELECT 
            mes,
            nome_produto,
            vendas,
            TO_CHAR(mes, 'YYYY-Q') || EXTRACT(QUARTER FROM mes) as trimestre
        FROM vendas_mensais
        WHERE rank <= 5  -- Top 5 por mês para não poluir o gráfico
        ORDER BY mes DESC, vendas DESC
    """
    
    df_sazonal = query_postgres(query_sazonal, pg_conn)

    if not df_sazonal.empty:
        # Pré-processamento
        df_sazonal['mes_formatado'] = df_sazonal['mes'].dt.strftime('%Y-%m')
        df_pivot = df_sazonal.pivot_table(index='nome_produto', 
                                        columns='mes_formatado', 
                                        values='vendas', 
                                        fill_value=0)
        
        # Normalização para melhor visualização (opcional)
        df_normalized = df_pivot.div(df_pivot.max(axis=1), axis=0)
        
        # Heatmap interativo
        fig = px.imshow(
            df_normalized,
            labels=dict(x="Mês", y="Produto", color="Vendas Normalizadas"),
            color_continuous_scale='tealrose',  # Escala de cores moderna
            aspect="auto",
            title='Padrões Sazonais: Vendas Normalizadas por Produto (Top 5 mensal)'
        )
        
        # Ajustes finais
        fig.update_layout(
            xaxis_title="Período",
            yaxis_title="Produto",
            coloraxis_colorbar=dict(title="Intensidade"),
            height=600  # Altura aumentada para melhor visualização
        )
        
        # Adicionar interatividade avançada
        fig.update_traces(
            hovertemplate="<b>%{y}</b><br>Mês: %{x}<br>Intensidade: %{z:.2f}<extra></extra>"
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Opcional: Gráfico de linha complementar para os top 3 produtos
        top_products = df_sazonal.groupby('nome_produto')['vendas'].sum().nlargest(3).index
        if len(top_products) > 0:
            st.markdown("### Tendência dos 3 Produtos Mais Vendidos")
            fig_line = px.line(
                df_sazonal[df_sazonal['nome_produto'].isin(top_products)],
                x='mes',
                y='vendas',
                color='nome_produto',
                line_shape='spline',
                markers=True,
                labels={'vendas': 'Vendas', 'mes': 'Mês'},
                title='Evolução Mensal'
            )
            fig_line.update_layout(hovermode='x unified')
            st.plotly_chart(fig_line, use_container_width=True)
            
    else:
        st.warning("Nenhum dado disponível para análise sazonal")
# Rodapé
st.markdown("---")
st.markdown("Dashboard desenvolvido para o Avaliação 2 de Computação Escalável - FGV 2025.1")
