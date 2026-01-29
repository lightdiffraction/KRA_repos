-- make denormalized view
CREATE OR REPLACE VIEW denormalized_page_data AS
SELECT 
    p.id as page_id,
    p.title,
    p.views,
    p.text,
    pr.name as project_name,
    n.name as namespace_name,
    string_agg(DISTINCT c.name, '; ') as categories,
    COUNT(DISTINCT upe.id) as edit_count,
    COUNT(DISTINCT pv.id) as view_count,
    COUNT(DISTINCT cm.id) as comment_count
FROM public.page p
LEFT JOIN public.project pr ON p.project_id = pr.id
LEFT JOIN public.namespace n ON p.namespace_id = n.id
LEFT JOIN public.page_category pc ON p.id = pc.page_id
LEFT JOIN public.category c ON pc.category_id = c.id
LEFT JOIN public.user_page_edit upe ON p.id = upe.page_id
LEFT JOIN public.page_view pv ON p.id = pv.page_id
LEFT JOIN public.comment cm ON p.id = cm.page_id
GROUP BY p.id, p.title, p.views, p.text, pr.name, n.name
LIMIT 10000;

-- EXPORT view
COPY (SELECT * FROM denormalized_page_data) TO 'C://Temp/denormalized_pages.csv' WITH CSV HEADER;