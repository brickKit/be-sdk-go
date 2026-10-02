package migrate

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	neturl "net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"testing/fstest"

	_ "github.com/jackc/pgx/v5/stdlib"
)

// testMigrations 是两条最小迁移：表名不带 schema 前缀，靠 search_path 落进 PG_SCHEMA。
var testMigrations = fstest.MapFS{
	"001_create_a.up.sql":   {Data: []byte("CREATE TABLE a (id int);")},
	"001_create_a.down.sql": {Data: []byte("DROP TABLE a;")},
	"002_create_b.up.sql":   {Data: []byte("CREATE TABLE b (id int);")},
	"002_create_b.down.sql": {Data: []byte("DROP TABLE b;")},
}

// testDB 用 TEST_PG_DSN（brickkit_test_db 的超级用户连接）开一个管理连接，建一个临时
// schema，测试结束删掉。返回管理连接、schema 名和指向它的六个 PG_* 键。
// 没有 TEST_PG_DSN 时在调用它的测试函数里 Skip——必须在测试 goroutine 里调用。
func testDB(t *testing.T) (*sql.DB, string, map[string]string) {
	t.Helper()
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("需要 TEST_PG_DSN（brickkit_test_db）")
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close() })
	schema := "migtest_" + randHex(t)
	if _, err := admin.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = admin.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE") })

	u, err := neturl.Parse(dsn)
	if err != nil {
		t.Fatal(err)
	}
	pw, _ := u.User.Password()
	env := map[string]string{
		"PG_HOST": u.Hostname(), "PG_PORT": u.Port(), "PG_DATABASE": strings.TrimPrefix(u.Path, "/"),
		"PG_USER": u.User.Username(), "PG_PASSWORD": pw, "PG_SCHEMA": schema,
	}
	return admin, schema, env
}

func randHex(t *testing.T) string {
	t.Helper()
	b := make([]byte, 4)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func tableExists(t *testing.T, db *sql.DB, schema, table string) bool {
	t.Helper()
	var n int
	err := db.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2`, schema, table).Scan(&n)
	if err != nil {
		t.Fatal(err)
	}
	return n == 1
}

func TestRunRejectsUnknownArgsBeforeReadingEnv(t *testing.T) {
	for _, args := range [][]string{nil, {}, {"apply"}, {"up", "x"}, {"UP"}} {
		err := Run(context.Background(), map[string]string{}, args, testMigrations)
		if !errors.Is(err, ErrUsage) {
			t.Fatalf("args=%q：应是用法错误（先查参数、不读环境），got %v", args, err)
		}
		if err != nil && strings.Contains(err.Error(), "PG_HOST") {
			t.Fatalf("args=%q：用法错误里不应出现缺键信息：%v", args, err)
		}
	}
}

func TestRunMissingKeyNamesTheKey(t *testing.T) {
	env := map[string]string{
		"PG_HOST": "127.0.0.1", "PG_PORT": "1", "PG_DATABASE": "x", "PG_USER": "u", "PG_PASSWORD": "p",
	}
	err := Run(context.Background(), env, []string{"up"}, testMigrations)
	if err == nil || !strings.Contains(err.Error(), "PG_SCHEMA") {
		t.Fatalf("缺 PG_SCHEMA 时错误应点名该键，got %v", err)
	}
	if errors.Is(err, ErrUsage) {
		t.Fatalf("缺键不是用法错误：%v", err)
	}

	err = Run(context.Background(), map[string]string{}, []string{"down"}, testMigrations)
	for _, k := range []string{"PG_HOST", "PG_PORT", "PG_DATABASE", "PG_USER", "PG_PASSWORD", "PG_SCHEMA"} {
		if err == nil || !strings.Contains(err.Error(), k) {
			t.Fatalf("空环境时错误应点名 %s，got %v", k, err)
		}
	}
}

func TestRunUpTwiceIsIdempotent(t *testing.T) {
	admin, schema, env := testDB(t)
	ctx := context.Background()
	if err := Run(ctx, env, []string{"up"}, testMigrations); err != nil {
		t.Fatalf("第一次 up：%v", err)
	}
	if err := Run(ctx, env, []string{"up"}, testMigrations); err != nil {
		t.Fatalf("第二次 up（ErrNoChange）不应报错：%v", err)
	}
	for _, tbl := range []string{"a", "b", "schema_migrations_" + schema} {
		if !tableExists(t, admin, schema, tbl) {
			t.Fatalf("表 %s 应落在 schema %s", tbl, schema)
		}
	}
	// 状态表只有这一张，不落在 public，名字里也没有字面的点号。
	var n int
	if err := admin.QueryRow(`SELECT count(*) FROM information_schema.tables WHERE table_name LIKE 'schema_migrations_' || $1 || '%'`, schema).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("状态表应恰好一张，got %d", n)
	}
	var version int
	var dirty bool
	if err := admin.QueryRow(fmt.Sprintf(`SELECT version, dirty FROM %s.schema_migrations_%s`, schema, schema)).Scan(&version, &dirty); err != nil {
		t.Fatal(err)
	}
	if version != 2 || dirty {
		t.Fatalf("状态应为 version=2 dirty=false，got %d %v", version, dirty)
	}
}

func TestRunDownThenUp(t *testing.T) {
	admin, schema, env := testDB(t)
	ctx := context.Background()
	if err := Run(ctx, env, []string{"up"}, testMigrations); err != nil {
		t.Fatalf("up：%v", err)
	}
	if err := Run(ctx, env, []string{"down"}, testMigrations); err != nil {
		t.Fatalf("down：%v", err)
	}
	if tableExists(t, admin, schema, "a") || tableExists(t, admin, schema, "b") {
		t.Fatal("down 之后 a、b 应都不存在")
	}
	if err := Run(ctx, env, []string{"down"}, testMigrations); err != nil {
		t.Fatalf("已全部回滚时再 down（ErrNoChange）不应报错：%v", err)
	}
	if err := Run(ctx, env, []string{"up"}, testMigrations); err != nil {
		t.Fatalf("再次 up：%v", err)
	}
	if !tableExists(t, admin, schema, "a") || !tableExists(t, admin, schema, "b") {
		t.Fatal("再次 up 之后 a、b 应都存在")
	}
}

func TestRunPasswordWithSpecialChars(t *testing.T) {
	admin, schema, env := testDB(t)
	role := "migtest_role_" + randHex(t)
	const pw = "p@ss:w/rd%"
	if _, err := admin.Exec(fmt.Sprintf(`CREATE ROLE %s LOGIN PASSWORD '%s'`, role, pw)); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_, _ = admin.Exec("DROP SCHEMA IF EXISTS " + schema + " CASCADE")
		_, _ = admin.Exec("DROP ROLE IF EXISTS " + role)
	})
	if _, err := admin.Exec(fmt.Sprintf(`ALTER SCHEMA %s OWNER TO %s`, schema, role)); err != nil {
		t.Fatal(err)
	}
	env["PG_USER"], env["PG_PASSWORD"] = role, pw
	if err := Run(context.Background(), env, []string{"up"}, testMigrations); err != nil {
		t.Fatalf("口令含 @ : / %% 时 up 应成功：%v", err)
	}
	if !tableExists(t, admin, schema, "schema_migrations_"+schema) {
		t.Fatal("状态表应落在该 schema")
	}
	// 反证：口令错了必须连不上，否则上面的成功说明不了口令被原样送到了服务端。
	env["PG_PASSWORD"] = "p@ss:w/rd"
	if err := Run(context.Background(), env, []string{"up"}, testMigrations); err == nil {
		t.Skip("服务端对这条连接不校验口令（trust），验证不了口令转义")
	}
}

// TestMainBadArgsExitsTwo 在子进程里跑 Main：参数不对时打印用法并以 2 退出，且不看环境。
func TestMainBadArgsExitsTwo(t *testing.T) {
	if os.Getenv("BE_MIGRATE_MAIN_CHILD") == "1" {
		os.Args = []string{"migrate", "apply"}
		Main(testMigrations)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMainBadArgsExitsTwo$")
	cmd.Env = []string{"BE_MIGRATE_MAIN_CHILD=1"}
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 2 {
		t.Fatalf("应以 2 退出，got err=%v 输出：%s", err, out)
	}
	if !strings.Contains(string(out), "up") || !strings.Contains(string(out), "down") {
		t.Fatalf("应打印用法，输出：%s", out)
	}
	if strings.Contains(string(out), "PG_HOST") {
		t.Fatalf("参数错误时不应去读环境，输出：%s", out)
	}
}

// TestMainMissingKeyExitsOne：参数对、缺连接键时以 1 退出并点名缺的键。
func TestMainMissingKeyExitsOne(t *testing.T) {
	if os.Getenv("BE_MIGRATE_MAIN_CHILD") == "2" {
		os.Args = []string{"migrate", "up"}
		Main(testMigrations)
		return
	}
	cmd := exec.Command(os.Args[0], "-test.run=^TestMainMissingKeyExitsOne$")
	cmd.Env = []string{"BE_MIGRATE_MAIN_CHILD=2", "PG_HOST=127.0.0.1", "PG_PORT=1", "PG_DATABASE=x", "PG_USER=u", "PG_PASSWORD=p"}
	out, err := cmd.CombinedOutput()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 1 {
		t.Fatalf("应以 1 退出，got err=%v 输出：%s", err, out)
	}
	if !strings.Contains(string(out), "PG_SCHEMA") {
		t.Fatalf("应点名 PG_SCHEMA，输出：%s", out)
	}
}
