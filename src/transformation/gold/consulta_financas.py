from __future__ import annotations

import os

# Ferramentas auxiliares para somatorios cumulativos usados nos graficos
from itertools import accumulate

# Spark + Delta serao acessados via SQL para leitura das views
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

try:
    import matplotlib.pyplot as plt
    from matplotlib.ticker import FuncFormatter

    try:
        plt.style.use("seaborn-v0_8-whitegrid")
    except OSError:  # compatibilidade com versões antigas
        plt.style.use("seaborn-whitegrid")
except ImportError:  # pragma: no cover - ambiente sem matplotlib
    plt = None
    FuncFormatter = None

# Caminhos utilizados para localizar as tabelas Delta e salvar os graficos
BASE_DIR = os.getcwd()
GOLD_DIR = os.path.join(BASE_DIR, "data", "gold", "financas")
ARTIFACTS_DIR = os.path.join(BASE_DIR, "artifacts")
GASTOS_CHART_PATH = os.path.join(ARTIFACTS_DIR, "gastos_evolucao_mensal_2025.png")
GASTOS_DONUT_PATH = os.path.join(ARTIFACTS_DIR, "gastos_donut_2025.png")
RENDA_DONUT_PATH = os.path.join(ARTIFACTS_DIR, "renda_donut_2025.png")

# Ordem cronologica usada nas consultas e legendas dos graficos
MESES_ORDENADOS = [
    "JANEIRO",
    "FEVEREIRO",
    "MARCO",
    "ABRIL",
    "MAIO",
    "JUNHO",
    "JULHO",
    "AGOSTO",
    "SETEMBRO",
    "OUTUBRO",
    "NOVEMBRO",
    "DEZEMBRO",
]

# Metadados das views: nome no catalogo e caminho Delta fisico
VIEWS = {
    "renda": {
        "table": "gold.vw_financas_renda_mensal",
        "delta_path": os.path.join(GOLD_DIR, "vw_renda_mensal"),
    },
    "gastos": {
        "table": "gold.vw_financas_gastos_mensal",
        "delta_path": os.path.join(GOLD_DIR, "vw_gastos_mensal"),
    },
}


def build_spark() -> SparkSession:
    """Inicializa uma sessao Spark configurada para ler tabelas Delta."""
    builder = (
        SparkSession.builder
        .appName("ConsultaGold_Financas_Renda")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql("USE gold")
    return spark


def ensure_table(spark: SparkSession, table: str, delta_path: str) -> None:
    """Registra a tabela no catalogo Spark se ainda nao estiver disponivel."""
    if not os.path.exists(delta_path):
        raise FileNotFoundError(f"Delta path não encontrado: {delta_path}")

    if spark.catalog.tableExists(table):
        return

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table}
        USING DELTA
        LOCATION 'file://{delta_path}'
        """
    )


def plot_gastos(spark: SparkSession, table: str, *, ano: int, output_path: str) -> None:
    """Consulta gastos mensais via SQL e gera grafico de barras + linha acumulada."""
    if plt is None or FuncFormatter is None:
        print("Matplotlib não está disponível; gráfico de gastos não gerado.")
        return

    monthly_sql = f"""
        SELECT
            {', '.join([f"COALESCE(SUM(`{mes}`), 0) AS `{mes}`" for mes in MESES_ORDENADOS])}
        FROM {table}
        WHERE ano = {ano} AND categoria = 'TOTAL'
    """

    row = spark.sql(monthly_sql).first()

    if row is None or all((row[mes] or 0) == 0 for mes in MESES_ORDENADOS):
        fallback_sql = f"""
            SELECT
                {', '.join([f"COALESCE(SUM(`{mes}`), 0) AS `{mes}`" for mes in MESES_ORDENADOS])}
            FROM {table}
            WHERE ano = {ano} AND categoria <> 'TOTAL'
        """
        row = spark.sql(fallback_sql).first()

    if row is None:
        print(f"Nenhum dado de gastos encontrado para o ano {ano}.")
        return

    monthly_values = [float((row[mes] or 0.0)) for mes in MESES_ORDENADOS]

    cumulative = list(accumulate(monthly_values))
    x_positions = list(range(len(MESES_ORDENADOS)))

    max_value = max(monthly_values) if monthly_values else 0.0
    bar_color = "#3B82F6"
    line_color = "#16A34A"

    fig, ax_bar = plt.subplots(figsize=(13, 5), dpi=140)
    ax_bar.set_facecolor("#f5f7fb")

    bars = ax_bar.bar(
        x_positions,
        monthly_values,
        color=bar_color,
        edgecolor="#1E3A8A",
        linewidth=0.6,
        alpha=0.9,
        label="Gastos do mês",
    )

    for bar in bars:
        bar.set_linewidth(0)
        bar.set_alpha(0.88)

    ax_bar.set_xticks(x_positions)
    ax_bar.set_xticklabels(MESES_ORDENADOS, rotation=40, ha="right")
    ax_bar.set_ylabel("Valor (R$)")
    ax_bar.set_xlabel("Mês")
    ax_bar.set_title(f"Evolução Mensal de Gastos - {ano}", fontsize=14, pad=12)
    ax_bar.yaxis.set_major_formatter(
        FuncFormatter(lambda x, _: f"{x:,.0f}".replace(",", "."))
    )
    ax_bar.set_ylim(0, max_value * 1.25 if max_value else 1)
    ax_bar.grid(axis="y", linestyle="--", alpha=0.25)

    ax_line = ax_bar.twinx()
    ax_line.plot(
        x_positions,
        cumulative,
        color=line_color,
        marker="o",
        markersize=6,
        linewidth=2.8,
        label="Acumulado",
    )
    ax_line.fill_between(
        x_positions,
        cumulative,
        color=line_color,
        alpha=0.18,
    )
    ax_line.set_ylabel("Acumulado (R$)")
    ax_line.yaxis.set_major_formatter(
        FuncFormatter(lambda x, _: f"{x:,.0f}".replace(",", "."))
    )
    ax_line.grid(False)

    for spine in ["top", "right"]:
        ax_bar.spines[spine].set_visible(False)
        ax_line.spines[spine].set_visible(False)

    linhas, labels = ax_bar.get_legend_handles_labels()
    linhas2, labels2 = ax_line.get_legend_handles_labels()
    ax_bar.legend(linhas + linhas2, labels + labels2, loc="upper left", frameon=False)

    plt.tight_layout(pad=1.2)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"Gráfico de gastos salvo em: {output_path}")


def plot_donut(
    spark: SparkSession,
    table: str,
    *,
    ano: int,
    is_gastos: bool,
    output_path: str,
    title: str,
) -> None:
    """Consulta totais anuais por categoria e monta grafico de rosca estilizado."""
    if plt is None:
        print("Matplotlib não está disponível; gráfico de pizza não gerado.")
        return

    totais_sql = f"""
        SELECT
            categoria,
            COALESCE(SUM(TOTAL), 0) AS valor_total
        FROM {table}
        WHERE ano = {ano} AND categoria <> 'TOTAL'
        GROUP BY categoria
        HAVING valor_total > 0
        ORDER BY valor_total DESC
    """

    totais_local = spark.sql(totais_sql).collect()

    if not totais_local:
        print(f"Não foi possível calcular os valores para {title} em {ano}.")
        return

    labels = [row["categoria"] for row in totais_local]
    valores = [float(row["valor_total"] or 0.0) for row in totais_local]
    total = sum(valores)

    if total <= 0:
        print(f"Total zero para {title} em {ano}; gráfico não gerado.")
        return

    cores_base = plt.cm.tab20.colors
    cores = [cores_base[i % len(cores_base)] for i in range(len(labels))]

    fig, ax = plt.subplots(figsize=(6.5, 6.5), dpi=140)
    wedges, texts, autotexts = ax.pie(
        valores,
        labels=None,
        autopct=lambda pct: f"{pct:.0f}%" if pct >= 0.5 else "",
        pctdistance=0.85,
        startangle=90,
        colors=cores,
        wedgeprops={"linewidth": 1.1, "edgecolor": "white"},
    )

    centre_circle = plt.Circle((0, 0), 0.68, fc="white")
    fig.gca().add_artist(centre_circle)

    cor_texto = "#DC2626" if is_gastos else "#1D4ED8"
    ax.text(
        0,
        0,
        f"{title.upper()}\n{total:,.0f}".replace(",", "."),
        ha="center",
        va="center",
        fontsize=18,
        fontweight="bold",
        color=cor_texto,
        linespacing=1.4,
    )

    ax.set_title(f"Distribuição {title.lower()} por categoria - {ano}", fontsize=13, pad=18)
    ax.axis("equal")

    legend_labels = [f"{lab} ({val:,.0f})".replace(",", ".") for lab, val in zip(labels, valores)]
    ax.legend(
        wedges,
        legend_labels,
        title="Categorias",
        loc="center left",
        bbox_to_anchor=(1, 0, 0.35, 1),
        frameon=False,
    )

    plt.tight_layout()
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    plt.savefig(output_path, bbox_inches="tight")
    plt.close(fig)
    print(f"Gráfico de pizza salvo em: {output_path}")


def main() -> None:
    """Executa as consultas SQL, mostra previas e gera os graficos solicitados."""
    spark = build_spark()

    try:
        for nome, cfg in VIEWS.items():
            table = cfg["table"]
            path = cfg["delta_path"]

            # Cria a tabela no catalogo se necessario (sem reler todo o dataset)
            ensure_table(spark, table, path)

            # Pre-visualiza os dados relevantes (gastos filtrados para 2025)
            where_clause = "WHERE ano = 2025" if nome == "gastos" else ""
            preview_sql = f"SELECT * FROM {table} {where_clause} ORDER BY ano, categoria LIMIT 20"

            print(f"===== {table} ({nome}) =====")
            spark.sql(preview_sql).show(20, truncate=False)

            if nome == "gastos":
                # Grafico evolutivo mensal e pizza de distribuicao de gastos
                plot_gastos(spark, table, ano=2025, output_path=GASTOS_CHART_PATH)
                plot_donut(
                    spark,
                    table,
                    ano=2025,
                    is_gastos=True,
                    output_path=GASTOS_DONUT_PATH,
                    title="Gastos",
                )
            elif nome == "renda":
                # Apenas donut para renda (mantem mesma estetica dos gastos)
                plot_donut(
                    spark,
                    table,
                    ano=2025,
                    is_gastos=False,
                    output_path=RENDA_DONUT_PATH,
                    title="Renda",
                )
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
