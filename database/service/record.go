package service

import (
	"fmt"
	"usercenter/config"
	"usercenter/database"
)

// RecordCountForSite 查询站点下 status 为 'using' 的实例个数
func RecordCountForSite(zoneID string, siteID string) (int, error) {

	query := fmt.Sprintf("SELECT COUNT(*) FROM instance_%s WHERE site_id = ? AND status = 'using'", zoneID)
	var count int
	err := database.DB.QueryRow(query, siteID).Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, err
}

// InsertRecord 插入记录到 records 表
func InsertRecord(zoneID string, siteID string, date string, instances int) error {
	tx, err := database.DB.Begin()
	if err != nil {
		return err
	}

	// 1. 查询record表中site的记录条数
	var count int
	checkQuery := fmt.Sprintf("SELECT COUNT(*) FROM record_%s WHERE site_id = ?", zoneID)
	if err := tx.QueryRow(checkQuery, siteID).Scan(&count); err != nil {
		tx.Rollback()
		return err
	}

	// 2. 判断记录条数是否超过阈值，如果是，就需要删除最早的一条记录
	if count >= config.MAXHISTORYNUMBER {
		deleteQuery := fmt.Sprintf("DELETE FROM record_%s WHERE site_id = ? ORDER BY date ASC LIMIT 1", zoneID)
		if _, err := tx.Exec(deleteQuery, siteID); err != nil {
			tx.Rollback()
			return err
		}
	}

	// 3. 插入最新的一条数据
	insertQuery := fmt.Sprintf("INSERT INTO record_%s (site_id, date, instances) VALUES (?, ?, ?)", zoneID)
	if _, err := tx.Exec(insertQuery, siteID, date, instances); err != nil {
		tx.Rollback()
		return err
	}

	// 提交事务
	if err := tx.Commit(); err != nil {
		tx.Rollback()
		return err
	}

	return nil
}
