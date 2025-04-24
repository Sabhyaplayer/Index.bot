// api/movies.js
import pkg from 'pg';

const { Pool } = pkg;

const pool = new Pool({
    connectionString: process.env.POSTGRES_URL, // <<< --- Reads from Vercel Env Var
    ssl: {
        rejectUnauthorized: false // Adjust based on your DB provider (often needed for Neon, Supabase, etc.)
    },
    // Optional: Pool configuration
    // max: 10,
    // idleTimeoutMillis: 30000,
    // connectionTimeoutMillis: 5000,
});

/**
 * Maps frontend sort keys to actual database column names or expressions.
 */
const mapSortColumn = (key) => {
    const mapping = {
        id: 'original_id',
        filename: 'lower(filename)',
        size: 'size_bytes',
        quality: 'quality',
        lastUpdated: 'last_updated_ts', // CRITICAL: Assumes 'last_updated_ts' TIMESTAMP/TIMESTAMPTZ column exists
    };
    return mapping[key] || 'last_updated_ts'; // Default sort
};

/**
 * Normalizes text for searching by converting to lowercase and REMOVING
 * common separators (._-) and spaces.
 * @param {string} text - The input text.
 * @returns {string} The normalized text with separators and spaces removed.
 */
const normalizeSearchTextForComparison = (text) => {
    if (!text) return '';
    return String(text)
        .toLowerCase()
        // Remove periods, underscores, hyphens, AND spaces
        .replace(/[._\s-]+/g, '') // Changed: Now removes spaces too (\s)
        .trim(); // Keep trim just in case, though replace should handle most cases
};


export default async function handler(request, response) {
    // CORS Headers - Important for allowing requests from your Vercel frontend URL
    // While Vercel routing often makes this seamless for same-origin, it's good practice.
    response.setHeader('Access-Control-Allow-Origin', '*'); // Be more specific in production if possible (e.g., your vercel domain)
    response.setHeader('Access-Control-Allow-Methods', 'GET, OPTIONS');
    response.setHeader('Access-Control-Allow-Headers', 'Content-Type');

    if (request.method === 'OPTIONS') {
        // Pre-flight request. Reply successfully:
        return response.status(200).end();
    }

    if (request.method !== 'GET') {
        response.setHeader('Allow', ['GET', 'OPTIONS']);
        return response.status(405).json({ error: `Method ${request.method} Not Allowed` });
    }

    let client;
    let queryParams = []; // Define outside try block for logging in finally

    try {
        console.log('API Request Received. Query:', request.query);

        const {
            search, quality, type,
            sort = 'lastUpdated', sortDir = 'desc',
            page = 1, limit = 50, id,
        } = request.query;

        const currentPage = Math.max(1, parseInt(page, 10) || 1);
        const currentLimit = Math.max(1, Math.min(100, parseInt(limit, 10) || 50)); // Limit max page size
        const offset = (currentPage - 1) * currentLimit;
        const sortColumn = mapSortColumn(sort);
        const sortDirection = sortDir?.toLowerCase() === 'asc' ? 'ASC' : 'DESC';

        console.log(`Parsed Params: page=${currentPage}, limit=${currentLimit}, offset=${offset}, sort=${sortColumn}, dir=${sortDirection}, search='${search}', quality='${quality}', type='${type}', id='${id}'`);

        // --- Build SQL Query ---
        let baseQuery = 'FROM movies WHERE 1=1'; // Ensure your table is named 'movies'
        queryParams = []; // Reset queryParams for this request
        let paramIndex = 1;

        // --- Filtering Logic ---
        if (id) {
            baseQuery += ` AND original_id = $${paramIndex++}`;
            queryParams.push(id);
            console.log(`Filtering by specific original_id: ${id}`);
        } else {
            if (search) {
                const searchTerm = search.trim();
                const isNumericSearch = /^\d+$/.test(searchTerm);

                // Check if search term IS purely numeric AND *maybe* corresponds to an ID
                // IMPORTANT: Decide if a purely numeric search should *only* match ID
                // or also search filenames containing numbers. Current logic prioritizes ID.
                if (isNumericSearch && searchTerm.length < 10) { // Heuristic: Assume long numbers aren't IDs
                    // Option 1: Search ID *only* if numeric
                    // baseQuery += ` AND original_id = $${paramIndex++}`;
                    // queryParams.push(parseInt(searchTerm, 10));
                    // console.log(`Numeric search detected. Querying for original_id: ${searchTerm}`);

                    // Option 2: Search ID *OR* normalized filename if numeric
                     const normalizedSearchTerm = normalizeSearchTextForComparison(searchTerm);
                     const normalizedDbFilename = `regexp_replace(lower(filename), '[._\\s-]+', '', 'g')`;
                     baseQuery += ` AND (original_id = $${paramIndex++} OR ${normalizedDbFilename} ILIKE $${paramIndex++})`;
                     queryParams.push(parseInt(searchTerm, 10));
                     queryParams.push(`%${normalizedSearchTerm}%`); // Match anywhere in normalized filename
                     console.log(`Numeric search. Querying for original_id: ${searchTerm} OR normalized filename like: %${normalizedSearchTerm}%`);

                } else {
                    // *** NORMALIZED TEXT SEARCH ***
                    const normalizedSearchTerm = normalizeSearchTextForComparison(searchTerm);
                    if (normalizedSearchTerm) {
                        const normalizedDbFilename = `regexp_replace(lower(filename), '[._\\s-]+', '', 'g')`;
                        baseQuery += ` AND ${normalizedDbFilename} ILIKE $${paramIndex++}`;
                        queryParams.push(`%${normalizedSearchTerm}%`); // Match anywhere
                        console.log(`Normalized text search. Comparing normalized filename with: %${normalizedSearchTerm}%`);
                    }
                }
            }

            // Apply quality filter
            if (quality) {
                baseQuery += ` AND quality = $${paramIndex++}`;
                queryParams.push(quality);
                console.log(`Applying quality filter: ${quality}`);
            }

            // Apply type filter
            if (type === 'movies') {
                baseQuery += ` AND is_series = FALSE`; // Ensure 'is_series' column exists and is BOOLEAN
                console.log(`Applying type filter: movies`);
            } else if (type === 'series') {
                baseQuery += ` AND is_series = TRUE`;
                console.log(`Applying type filter: series`);
            }
        } // End if (!id)

        // --- Execute Database Queries ---
        client = await pool.connect();
        console.log('Database client connected successfully.');

        // 1. Count Query
        let totalItems = 1; // Default for specific ID request
        if (!id) {
            const countSql = `SELECT COUNT(*) ${baseQuery}`;
            console.log('Executing Count SQL:', countSql, 'Params:', queryParams);
            const countResult = await client.query(countSql, queryParams);
            totalItems = parseInt(countResult.rows[0].count, 10);
            console.log('Total items found for query:', totalItems);
            // Handle case where page requested is beyond results
            if (offset >= totalItems && totalItems > 0) {
                 console.warn(`Requested offset ${offset} is >= total items ${totalItems}. Returning empty items.`);
                 // No need to run data query if offset is too high
                 response.setHeader('Content-Type', 'application/json');
                 return response.status(200).json({
                     items: [],
                     totalItems: totalItems,
                     page: currentPage,
                     totalPages: Math.ceil(totalItems / currentLimit),
                     limit: currentLimit,
                     filters: { search, quality, type },
                     sorting: { sort: sort, sortDir: sortDir }
                 });
            }
        }

        // 2. Data Query
        // Ensure all required columns exist in your 'movies' table:
        // original_id, filename, size_bytes, quality, last_updated_ts, is_series, url,
        // telegram_link, gdflix_link, hubcloud_link, filepress_link, gdtot_link,
        // languages, originalFilename (if used in JS)
        let dataSql = `SELECT * ${baseQuery}`; // Select all columns explicitly if needed

        if (!id) {
            // Add NULLS FIRST/LAST if needed for certain columns, e.g., size_bytes might be null
            const orderByClause = `ORDER BY ${sortColumn} ${sortDirection} ${sortColumn === 'size_bytes' ? 'NULLS LAST' : ''}, original_id ${sortDirection}`;
            dataSql += ` ${orderByClause}`;
            dataSql += ` LIMIT $${paramIndex++} OFFSET $${paramIndex++}`;
            queryParams.push(currentLimit, offset);
        } else {
            dataSql += ` LIMIT 1`; // Ensure only one result for specific ID
        }

        console.log('Executing Data SQL:', dataSql, 'Params:', queryParams);
        const dataResult = await client.query(dataSql, queryParams);
        const items = dataResult.rows;
        console.log(`Fetched ${items.length} item(s).`);

        // --- Format and Send JSON Response ---
        const totalPages = id ? 1 : Math.ceil(totalItems / currentLimit);
        console.log(`Calculated totalPages: ${totalPages} (totalItems: ${totalItems}, limit: ${currentLimit})`);

        response.setHeader('Content-Type', 'application/json');
        response.status(200).json({
            items: items,
            totalItems: totalItems,
            page: currentPage,
            totalPages: totalPages,
            limit: currentLimit,
            filters: { search, quality, type },
            sorting: { sort: sort, sortDir: sortDir }
        });

    } catch (error) {
        console.error('!!! API Database Error:', error);
        if (error.message && error.message.includes('last_updated_ts')) {
             console.error(">>> Potential issue with 'last_updated_ts' column. Check schema. <<<");
        }
        if (error.message && error.message.includes('is_series')) {
             console.error(">>> Potential issue with 'is_series' column. Check schema (should be BOOLEAN). <<<");
        }
         if (error.message && error.message.includes('size_bytes')) {
             console.error(">>> Potential issue with 'size_bytes' column. Check schema (should be NUMERIC/BIGINT). <<<");
        }
        // Log the query params even on error
        console.error("Failing Params (approximate):", queryParams);

        response.status(500).json({
            error: 'Failed to fetch movie data from database.',
            details: process.env.NODE_ENV === 'development' ? error.message : 'Internal Server Error. Check API logs.' // Don't expose detailed errors in production
        });
    } finally {
        if (client) {
            client.release();
            console.log('Database client released.');
        }
    }
}