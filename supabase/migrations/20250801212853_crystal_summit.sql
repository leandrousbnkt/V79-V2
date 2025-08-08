/*
  # Criar tabela de análises

  1. Nova Tabela
    - `analyses`
      - `id` (uuid, primary key)
      - `segmento` (text)
      - `produto` (text)
      - `publico` (text)
      - `preco` (numeric)
      - `objetivo_receita` (numeric)
      - `orcamento_marketing` (numeric)
      - `prazo_lancamento` (text)
      - `concorrentes` (text)
      - `dados_adicionais` (text)
      - `query` (text)
      - `status` (text)
      - `avatar_data` (jsonb)
      - `positioning_data` (jsonb)
      - `competition_data` (jsonb)
      - `marketing_data` (jsonb)
      - `metrics_data` (jsonb)
      - `funnel_data` (jsonb)
      - `action_plan_data` (jsonb)
      - `insights_data` (jsonb)
      - `drivers_mentais_data` (jsonb)
      - `provas_visuais_data` (jsonb)
      - `anti_objecao_data` (jsonb)
      - `pre_pitch_data` (jsonb)
      - `predicoes_futuro_data` (jsonb)
      - `pesquisa_web_data` (jsonb)
      - `comprehensive_analysis` (jsonb)
      - `local_files_path` (text)
      - `created_at` (timestamptz)
      - `updated_at` (timestamptz)

  2. Segurança
    - Habilitar RLS na tabela `analyses`
    - Adicionar política para usuários autenticados lerem seus próprios dados
    - Adicionar política para inserção de dados
*/

CREATE TABLE IF NOT EXISTS analyses (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  segmento text NOT NULL,
  produto text,
  publico text,
  preco numeric,
  objetivo_receita numeric,
  orcamento_marketing numeric,
  prazo_lancamento text,
  concorrentes text,
  dados_adicionais text,
  query text,
  status text DEFAULT 'processing',
  
  -- Dados estruturados das análises
  avatar_data jsonb,
  positioning_data jsonb,
  competition_data jsonb,
  marketing_data jsonb,
  metrics_data jsonb,
  funnel_data jsonb,
  action_plan_data jsonb,
  insights_data jsonb,
  
  -- Novos sistemas ultra-detalhados
  drivers_mentais_data jsonb,
  provas_visuais_data jsonb,
  anti_objecao_data jsonb,
  pre_pitch_data jsonb,
  predicoes_futuro_data jsonb,
  pesquisa_web_data jsonb,
  
  -- Análise completa
  comprehensive_analysis jsonb,
  
  -- Caminho dos arquivos locais
  local_files_path text,
  
  -- Timestamps
  created_at timestamptz DEFAULT now(),
  updated_at timestamptz DEFAULT now()
);

-- Habilitar RLS
ALTER TABLE analyses ENABLE ROW LEVEL SECURITY;

-- Política para leitura (todos podem ler por enquanto - ajustar conforme necessário)
CREATE POLICY "Permitir leitura de análises"
  ON analyses
  FOR SELECT
  USING (true);

-- Política para inserção (todos podem inserir por enquanto - ajustar conforme necessário)
CREATE POLICY "Permitir inserção de análises"
  ON analyses
  FOR INSERT
  WITH CHECK (true);

-- Política para atualização (todos podem atualizar por enquanto - ajustar conforme necessário)
CREATE POLICY "Permitir atualização de análises"
  ON analyses
  FOR UPDATE
  USING (true);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_analyses_segmento ON analyses(segmento);
CREATE INDEX IF NOT EXISTS idx_analyses_status ON analyses(status);
CREATE INDEX IF NOT EXISTS idx_analyses_created_at ON analyses(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_analyses_local_files ON analyses(local_files_path);

-- Função para atualizar updated_at automaticamente
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger para atualizar updated_at
DROP TRIGGER IF EXISTS update_analyses_updated_at ON analyses;
CREATE TRIGGER update_analyses_updated_at
    BEFORE UPDATE ON analyses
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();