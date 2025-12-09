package main

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

// ------------------------------
// Models
// ------------------------------

type BulkRow struct {
	ID          int64          `db:"id"`
	ArchiveFile sql.NullString `db:"archive_file"`
}

type PartnerRow struct {
	PartnerID int64          `db:"partner_id"`
	Meta      sql.NullString `db:"meta"`
}

type ClientRow struct {
	ClientID                 int64          `db:"client_id"`
	ClientContractAttachment sql.NullString `db:"client_contract_attachment_url"`
	ClientTaxAttachment      sql.NullString `db:"client_tax_attachment"`
	ClientPksAttachment      sql.NullString `db:"client_pks_attachment"`
}

// ------------------------------
// Global config
// ------------------------------

var (
	hydraSignPrefix string
)

var bulkS3Prefix string

// ------------------------------
// Init
// ------------------------------

func init() {
	if err := loadDotEnvFile(".env"); err != nil && !os.IsNotExist(err) {
		log.Fatalf("failed to load .env: %v", err)
	}

	// Hydra sign prefix (for client attachments)
	hydraSignPrefix = os.Getenv("HYDRA_SIGN_PREFIX")
	if hydraSignPrefix == "" {
		// default for safety
		hydraSignPrefix = "https://api.dev-genesis.lionparcel.com/hydra/v1/asset/sign?"
	}

	// Bulk S3 prefix (for bulk.archive_file)
	// Example:
	// - dev: https://dev-genesis.s3.ap-southeast-1.amazonaws.com/
	// - prod: https://genesis.s3.ap-southeast-1.amazonaws.com/
	bulkS3Prefix = os.Getenv("BULK_S3_PREFIX")
	if bulkS3Prefix == "" {
		// safe default for local/dev usage; override via env in real envs
		bulkS3Prefix = "https://dev-genesis.s3.ap-southeast-1.amazonaws.com/"
	}
}

// ------------------------------
// Main
// ------------------------------

func main() {
	ctx := context.Background()

	dsn := os.Getenv("DB_DSN")
	if dsn == "" {
		log.Fatal("DB_DSN env is required")
	}

	dryRun := os.Getenv("DRY_RUN") == "1"
	batchSize := loadBatchSizeFromEnv("BATCH_SIZE", 200)

	db, err := sqlx.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("ping db: %v", err)
	}

	log.Printf("starting REMOVE TAGGING migration (dryRun=%v, batchSize=%d)", dryRun, batchSize)

	if err := migrateBulkRemoveTag(ctx, db, dryRun, batchSize); err != nil {
		log.Fatalf("bulk migration failed: %v", err)
	}

	if err := migratePartnerRemoveTag(ctx, db, dryRun, batchSize); err != nil {
		log.Fatalf("partner migration failed: %v", err)
	}

	if err := migrateClientRemoveTag(ctx, db, dryRun, batchSize); err != nil {
		log.Fatalf("client migration failed: %v", err)
	}

	log.Println("remove tagging migration finished successfully")
}

// ------------------------------
// BULK: remove tagging in archive_file
// ------------------------------

func migrateBulkRemoveTag(ctx context.Context, db *sqlx.DB, dryRun bool, batchSize int) error {
	log.Println("== BULK: start remove tagging in archive_file ==")

	var (
		lastID       int64
		batchNum     int
		totalRows    int
		totalUpdated int
		totalSkipped int
	)

	for {
		rows, err := fetchBulkBatch(ctx, db, lastID, batchSize)
		if err != nil {
			return fmt.Errorf("fetch bulk batch: %w", err)
		}
		if len(rows) == 0 {
			log.Printf("[BULK] no more rows after id=%d, stopping", lastID)
			break
		}

		batchNum++
		log.Printf("[BULK] batch #%d, size=%d, id range %d..%d",
			batchNum, len(rows), rows[0].ID, rows[len(rows)-1].ID)

		for _, r := range rows {
			totalRows++
			lastID = r.ID

			updated, skipped, err := processBulkRowRemoveTag(ctx, db, r, dryRun)
			if err != nil {
				log.Printf("[BULK][ERROR] id=%d: %v", r.ID, err)
				continue
			}
			if updated {
				totalUpdated++
			}
			if skipped {
				totalSkipped++
			}
		}
	}

	log.Printf("[BULK][SUMMARY] totalRows=%d totalUpdated=%d totalSkipped=%d", totalRows, totalUpdated, totalSkipped)
	return nil
}

func fetchBulkBatch(ctx context.Context, db *sqlx.DB, lastID int64, limit int) ([]BulkRow, error) {
	query := `
SELECT
    id,
    archive_file
FROM bulk
WHERE
    id > ?
    AND archive_type = 'custom_client_rate'
    AND created_at >= DATE_SUB(NOW(), INTERVAL 1 MONTH)
    AND archive_file IS NOT NULL
    AND archive_file != ''
ORDER BY id ASC
LIMIT ?
`
	var rows []BulkRow
	if err := db.SelectContext(ctx, &rows, query, lastID, limit); err != nil {
		return nil, err
	}
	return rows, nil
}

func processBulkRowRemoveTag(
	ctx context.Context,
	db *sqlx.DB,
	row BulkRow,
	dryRun bool,
) (updated bool, skipped bool, err error) {
	if !row.ArchiveFile.Valid {
		return false, true, nil
	}
	raw := strings.TrimSpace(row.ArchiveFile.String)
	if raw == "" {
		return false, true, nil
	}
	newURL, changed := removeTagParamsFromURL(raw)
	if !changed {
		return false, true, nil
	}

	// Normalize to use env-based S3 prefix for bulk files
	newURL = normalizeBulkArchiveURL(newURL)

	if dryRun {
		log.Printf("[BULK][DRY-RUN] id=%d archive_file\nold=%s\nnew=%s", row.ID, raw, newURL)
		return false, false, nil
	}

	if err := updateBulkArchiveFile(ctx, db, row.ID, newURL); err != nil {
		return false, false, fmt.Errorf("update DB: %w", err)
	}

	log.Printf("[BULK][OK] id=%d updated archive_file\nold=%s\nnew=%s", row.ID, raw, newURL)
	return true, false, nil
}

func updateBulkArchiveFile(ctx context.Context, db *sqlx.DB, id int64, newURL string) error {
	query := `
UPDATE bulk
SET archive_file = ?
WHERE id = ?
`
	_, err := db.ExecContext(ctx, query, newURL, id)
	return err
}

// normalizeBulkArchiveURL rebuilds the bulk archive URL using the BULK_S3_PREFIX env,
// keeping only the filename part. If parsing fails, it returns the input as-is.
func normalizeBulkArchiveURL(rawURL string) string {
	if bulkS3Prefix == "" || rawURL == "" {
		return rawURL
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}

	// Take only the last segment (file name), e.g. bulk_upload_client_rate_1754324774.xlsx
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	if len(parts) == 0 {
		return rawURL
	}
	filename := parts[len(parts)-1]
	if filename == "" {
		return rawURL
	}

	prefix := strings.TrimRight(bulkS3Prefix, "/")
	return prefix + "/" + filename
}

// ------------------------------
// PARTNER: remove tagging in meta.partner_pos_attach_files[]
// ------------------------------

func migratePartnerRemoveTag(ctx context.Context, db *sqlx.DB, dryRun bool, batchSize int) error {
	log.Println("== PARTNER: start remove tagging in meta.partner_pos_attach_files ==")

	var (
		lastID       int64
		batchNum     int
		totalRows    int
		totalUpdated int
		totalSkipped int
	)

	for {
		rows, err := fetchPartnerBatch(ctx, db, lastID, batchSize)
		if err != nil {
			return fmt.Errorf("fetch partner batch: %w", err)
		}
		if len(rows) == 0 {
			log.Printf("[PARTNER] no more rows after partner_id=%d, stopping", lastID)
			break
		}

		batchNum++
		log.Printf("[PARTNER] batch #%d, size=%d, partner_id range %d..%d",
			batchNum, len(rows), rows[0].PartnerID, rows[len(rows)-1].PartnerID)

		for _, r := range rows {
			totalRows++
			lastID = r.PartnerID

			updated, skipped, err := processPartnerRowRemoveTag(ctx, db, r, dryRun)
			if err != nil {
				log.Printf("[PARTNER][ERROR] partner_id=%d: %v", r.PartnerID, err)
				continue
			}
			if updated {
				totalUpdated++
			}
			if skipped {
				totalSkipped++
			}
		}
	}

	log.Printf("[PARTNER][SUMMARY] totalRows=%d totalUpdated=%d totalSkipped=%d", totalRows, totalUpdated, totalSkipped)
	return nil
}

func fetchPartnerBatch(ctx context.Context, db *sqlx.DB, lastID int64, limit int) ([]PartnerRow, error) {
	query := `
SELECT
    partner_id,
    meta
FROM partner
WHERE
    partner_id > ?
    AND partner_is_banned != 1
    AND partner_contract_end >= NOW()
ORDER BY partner_id ASC
LIMIT ?
`
	var rows []PartnerRow
	if err := db.SelectContext(ctx, &rows, query, lastID, limit); err != nil {
		return nil, err
	}
	return rows, nil
}

func processPartnerRowRemoveTag(
	ctx context.Context,
	db *sqlx.DB,
	row PartnerRow,
	dryRun bool,
) (updated bool, skipped bool, err error) {
	if !row.Meta.Valid {
		return false, true, nil
	}

	rawMeta := strings.TrimSpace(row.Meta.String)
	if rawMeta == "" {
		return false, true, nil
	}

	var metaMap map[string]interface{}
	if err := json.Unmarshal([]byte(rawMeta), &metaMap); err != nil {
		log.Printf("[PARTNER][WARN] partner_id=%d invalid JSON meta, skip: %v", row.PartnerID, err)
		return false, true, nil
	}

	val, ok := metaMap["partner_pos_attach_files"]
	if !ok {
		return false, true, nil
	}

	files, ok := val.([]interface{})
	if !ok || len(files) == 0 {
		return false, true, nil
	}

	changed := false
	newFiles := make([]interface{}, 0, len(files))

	for _, item := range files {
		s, ok := item.(string)
		if !ok {
			newFiles = append(newFiles, item)
			continue
		}
		newURL, modified := removeTagParamsFromURL(s)
		if modified {
			changed = true
			newFiles = append(newFiles, newURL)
		} else {
			newFiles = append(newFiles, s)
		}
	}

	if !changed {
		return false, true, nil
	}

	metaMap["partner_pos_attach_files"] = newFiles

	newMetaBytes, err := json.Marshal(metaMap)
	if err != nil {
		return false, false, fmt.Errorf("marshal updated meta: %w", err)
	}
	newMeta := string(newMetaBytes)

	if dryRun {
		log.Printf("[PARTNER][DRY-RUN] partner_id=%d meta\nold=%s\nnew=%s", row.PartnerID, rawMeta, newMeta)
		return false, false, nil
	}

	if err := updatePartnerMeta(ctx, db, row.PartnerID, newMeta); err != nil {
		return false, false, fmt.Errorf("update DB: %w", err)
	}

	log.Printf("[PARTNER][OK] partner_id=%d updated meta (partner_pos_attach_files cleaned)", row.PartnerID)
	return true, false, nil
}

func updatePartnerMeta(ctx context.Context, db *sqlx.DB, partnerID int64, newMeta string) error {
	query := `
UPDATE partner
SET meta = ?
WHERE partner_id = ?
`
	_, err := db.ExecContext(ctx, query, newMeta, partnerID)
	return err
}

// ------------------------------
// CLIENT: remove ?tag=... from attachment URLs
// ------------------------------

func migrateClientRemoveTag(ctx context.Context, db *sqlx.DB, dryRun bool, batchSize int) error {
	log.Println("== CLIENT: start remove tagging in attachment URLs ==")

	var (
		lastID       int64
		batchNum     int
		totalRows    int
		totalUpdated int
		totalSkipped int
	)

	like := hydraSignPrefix + "%"

	for {
		rows, err := fetchClientBatch(ctx, db, lastID, batchSize, like)
		if err != nil {
			return fmt.Errorf("fetch client batch: %w", err)
		}
		if len(rows) == 0 {
			log.Printf("[CLIENT] no more rows after client_id=%d, stopping", lastID)
			break
		}

		batchNum++
		log.Printf("[CLIENT] batch #%d, size=%d, client_id range %d..%d",
			batchNum, len(rows), rows[0].ClientID, rows[len(rows)-1].ClientID)

		for _, r := range rows {
			totalRows++
			lastID = r.ClientID

			updated, skipped, err := processClientRowRemoveTag(ctx, db, r, dryRun)
			if err != nil {
				log.Printf("[CLIENT][ERROR] client_id=%d: %v", r.ClientID, err)
				continue
			}
			if updated {
				totalUpdated++
			}
			if skipped {
				totalSkipped++
			}
		}
	}

	log.Printf("[CLIENT][SUMMARY] totalRows=%d totalUpdated=%d totalSkipped=%d", totalRows, totalUpdated, totalSkipped)
	return nil
}

func fetchClientBatch(ctx context.Context, db *sqlx.DB, lastID int64, limit int, likePrefix string) ([]ClientRow, error) {
	query := `
SELECT
    client_id,
    client_contract_attachment_url,
    client_tax_attachment,
    client_pks_attachment
FROM client
WHERE
    client_id > ?
    AND (
        client_contract_attachment_url LIKE ? OR
        client_tax_attachment LIKE ? OR
        client_pks_attachment LIKE ?
    ) AND client_is_banned != 1 AND client_contract_end_date >= NOW()
ORDER BY client_id ASC
LIMIT ?
`
	var rows []ClientRow
	if err := db.SelectContext(ctx, &rows, query, lastID, likePrefix, likePrefix, likePrefix, limit); err != nil {
		return nil, err
	}
	return rows, nil
}

func processClientRowRemoveTag(
	ctx context.Context,
	db *sqlx.DB,
	row ClientRow,
	dryRun bool,
) (updated bool, skipped bool, err error) {
	updates := make(map[string]string)

	handleCol := func(col string, v sql.NullString) {
		if !v.Valid {
			return
		}
		raw := strings.TrimSpace(v.String)
		if raw == "" {
			return
		}
		// Hanya sentuh hydra URLs (safety)
		if !strings.HasPrefix(raw, hydraSignPrefix) {
			return
		}
		newURL, changed := removeTagParamsFromURL(raw)
		if changed {
			updates[col] = newURL
		}
	}

	handleCol("client_contract_attachment_url", row.ClientContractAttachment)
	handleCol("client_tax_attachment", row.ClientTaxAttachment)
	handleCol("client_pks_attachment", row.ClientPksAttachment)

	if len(updates) == 0 {
		return false, true, nil
	}

	if dryRun {
		log.Printf("[CLIENT][DRY-RUN] client_id=%d DB updates: %+v", row.ClientID, updates)
		return false, false, nil
	}

	if err := applyClientUpdates(ctx, db, row.ClientID, updates); err != nil {
		return false, false, fmt.Errorf("update DB: %w", err)
	}

	log.Printf("[CLIENT][OK] client_id=%d updated columns: %s", row.ClientID, strings.Join(mapKeys(updates), ", "))
	return true, false, nil
}

func applyClientUpdates(ctx context.Context, db *sqlx.DB, clientID int64, updates map[string]string) error {
	if len(updates) == 0 {
		return nil
	}

	setParts := make([]string, 0, len(updates))
	args := make([]interface{}, 0, len(updates)+1)

	for col, val := range updates {
		setParts = append(setParts, fmt.Sprintf("%s = ?", col))
		args = append(args, val)
	}

	args = append(args, clientID)

	query := fmt.Sprintf(`UPDATE client SET %s WHERE client_id = ?`, strings.Join(setParts, ", "))
	_, err := db.ExecContext(ctx, query, args...)
	return err
}

// ------------------------------
// URL helper
// ------------------------------

// removeTagParamsFromURL removes "tag" and "tagging" query params if present.
// Returns (newURL, changed).
func removeTagParamsFromURL(rawURL string) (string, bool) {
	if rawURL == "" {
		return rawURL, false
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		// keep as-is on parse error
		return rawURL, false
	}

	q := u.Query()
	changed := false

	if _, ok := q["tag"]; ok {
		q.Del("tag")
		changed = true
	}
	if _, ok := q["tagging"]; ok {
		q.Del("tagging")
		changed = true
	}

	if !changed {
		return rawURL, false
	}

	u.RawQuery = q.Encode()
	return u.String(), true
}

// ------------------------------
// Utils
// ------------------------------

func loadDotEnvFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, "=")
		if idx <= 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		if key == "" {
			continue
		}
		value := strings.TrimSpace(line[idx+1:])
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') ||
				(value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}
		_ = os.Setenv(key, value)
	}
	return scanner.Err()
}

func loadBatchSizeFromEnv(key string, def int) int {
	val := strings.TrimSpace(os.Getenv(key))
	if val == "" {
		return def
	}
	n, err := strconv.Atoi(val)
	if err != nil || n <= 0 {
		log.Printf("[WARN] invalid %s=%q, using default=%d", key, val, def)
		return def
	}
	return n
}

func mapKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
