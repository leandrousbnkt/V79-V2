/*
  # Criar tabela de arquivos de análise

  1. Nova Tabela
    - `analysis_files`
      - `id` (uuid, primary key)
      - `analysis_id` (uuid, foreign key)
      - `file_type` (text) - tipo do arquivo (avatar, drivers, provas, etc.)
      - `file_name` (text) - nome do arquivo
      - `file_path` (text) - caminho do arquivo
      - `file_size` (bigint) - tamanho do arquivo
      - `content_preview` (text) - preview do conteúdo
      - `created_at` (timestamptz)

  2. Segurança
    - Habilitar RLS na tabela `analysis_files`
    - Adicionar políticas de acesso
*/

CREATE TABLE IF NOT EXISTS analysis_files (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  analysis_id uuid NOT NULL REFERENCES analyses(id) ON DELETE CASCADE,
  file_type text NOT NULL,
  file_name text NOT NULL,
  file_path text NOT NULL,
  file_size bigint DEFAULT 0,
  content_preview text,
  created_at timestamptz DEFAULT now()
);

-- Habilitar RLS
ALTER TABLE analysis_files ENABLE ROW LEVEL SECURITY;

-- Política para leitura
CREATE POLICY "Permitir leitura de arquivos de análise"
  ON analysis_files
  FOR SELECT
  USING (true);

-- Política para inserção
CREATE POLICY "Permitir inserção de arquivos de análise"
  ON analysis_files
  FOR INSERT
  WITH CHECK (true);

-- Política para exclusão
CREATE POLICY "Permitir exclusão de arquivos de análise"
  ON analysis_files
  FOR DELETE
  USING (true);

-- Índices
CREATE INDEX IF NOT EXISTS idx_analysis_files_analysis_id ON analysis_files(analysis_id);
CREATE INDEX IF NOT EXISTS idx_analysis_files_type ON analysis_files(file_type);
CREATE INDEX IF NOT EXISTS idx_analysis_files_created_at ON analysis_files(created_at DESC);