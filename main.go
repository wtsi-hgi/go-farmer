package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/deneonet/benc/bpre"
	lru "github.com/hashicorp/golang-lru/v2"
	es "github.com/wtsi-hgi/go-farmer/elasticsearch"
	"gopkg.in/yaml.v3"
)

const (
	index             = "user-data-ssg-isg-lsf-analytics-*"
	maxSize           = 10000
	cacheSize         = 128
	bucketName        = "hits"
	dateFormat        = "2006/01/02"
	dbDirPerms        = 0770
	fileBufferSize    = 4 * 1024 * 1024
	desiredFlatDBSize = 32 * 1024 * 1024

	timeStampLength        = 8
	bomMaxWidth            = 34
	accountingNameMaxWidth = 24
	userNameMaxWidth       = 12
	notInGPUQueue          = byte(1)
	inGPUQueue             = byte(2)
	lengthEncodeLength     = 4
	detailsBufferLength    = 16 * 1024

	testPeriod = 1 // 1 is 10 mins, 2 is 3 days, otherwise over a month
	debug      = false
)

type YAMLConfig struct {
	Elastic struct {
		Host     string
		Username string
		Password string
		Scheme   string
		Port     int
	}
}

func main() {
	data, err := os.ReadFile(os.Args[1])
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	c := YAMLConfig{}

	err = yaml.Unmarshal([]byte(data), &c)
	if err != nil {
		log.Fatalf("error: %v", err)
	}

	cfg := es.Config{
		Host:     c.Elastic.Host,
		Port:     c.Elastic.Port,
		Scheme:   c.Elastic.Scheme,
		Username: c.Elastic.Username,
		Password: c.Elastic.Password,
	}

	client, err := es.NewClient(cfg)
	if err != nil {
		log.Fatalf("%s\n", err)
	}

	l, err := lru.New[string, *es.Result](cacheSize)
	if err != nil {
		log.Fatalf("%s\n", err)
	}

	dbPath := os.Args[2]

	dbExisted := false

	if _, err = os.Stat(dbPath); err == nil {
		dbExisted = true
	}

	if !dbExisted {
		err = initDB(dbPath, client)
		if err != nil {
			log.Fatalf("%s\n", err)
		}

		return
	}

	t := time.Now()

	bomQuery := &es.Query{
		Aggs: &es.Aggs{
			Stats: es.AggsStats{
				MultiTerms: es.MultiTerms{
					Terms: []es.Field{
						{Field: "ACCOUNTING_NAME"},
						{Field: "NUM_EXEC_PROCS"},
						{Field: "Job"},
					},
					Size: 1000,
				},
				Aggs: map[string]es.AggsField{
					"cpu_avail_sec": {
						Sum: &es.Field{Field: "AVAIL_CPU_TIME_SEC"},
					},
					"cpu_wasted_sec": {
						Sum: &es.Field{Field: "WASTED_CPU_SECONDS"},
					},
					"mem_avail_mb_sec": {
						Sum: &es.Field{Field: "MEM_REQUESTED_MB_SEC"},
					},
					"mem_wasted_mb_sec": {
						Sum: &es.Field{Field: "WASTED_MB_SECONDS"},
					},
					"wasted_cost": {
						ScriptedMetric: &es.ScriptedMetric{
							InitScript:    "state.costs = []",
							MapScript:     "double cpu_cost = doc.WASTED_CPU_SECONDS.value * params.cpu_second; double mem_cost = doc.WASTED_MB_SECONDS.value * params.mb_second; state.costs.add(Math.max(cpu_cost, mem_cost))",
							CombineScript: "double total = 0; for (t in state.costs) { total += t } return total",
							ReduceScript:  "double total = 0; for (a in states) { total += a } return total",
							Params: map[string]float64{
								"cpu_second": 7.0556e-07,
								"mb_second":  5.8865e-11,
							},
						},
					},
				},
			},
		},
		Query: &es.QueryFilter{
			Bool: es.QFBool{
				Filter: es.Filter{
					{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
					{"range": map[string]interface{}{
						"timestamp": map[string]string{
							"lte":    "2024-06-04T00:00:00Z",
							"gte":    "2024-05-04T00:00:00Z",
							"format": "strict_date_optional_time",
						},
					}},
					{"match_phrase": map[string]interface{}{"BOM": "Human Genetics"}},
				},
			},
		},
	}

	result, err := Search(l, client, index, bomQuery)
	if err != nil {
		log.Fatalf("Error searching: %s", err)
	}

	if len(result.HitSet.Hits) > 0 {
		fmt.Printf("num hits: %+v\n", len(result.HitSet.Hits))
		// fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0].Details)
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		fmt.Printf("num aggs: %+v\n", len(result.Aggregations.Stats.Buckets))
		// fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	}
	fmt.Printf("took: %s\n\n", time.Since(t))

	lte := "2024-06-04T00:00:00Z"
	gte := "2024-05-04T00:00:00Z"
	if testPeriod == 1 {
		lte = "2024-06-09T23:55:00Z"
		gte = "2024-06-09T23:50:00Z"
	} else if testPeriod == 2 {
		gte = "2024-06-03T00:00:00Z"
	}

	filter := es.Filter{
		{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
		{"range": map[string]interface{}{
			"timestamp": map[string]string{
				"lte":    lte,
				"gte":    gte,
				"format": "strict_date_optional_time",
			},
		}},
		{"match_phrase": map[string]interface{}{"BOM": "Human Genetics"}},
		{"match_phrase": map[string]interface{}{"ACCOUNTING_NAME": "hgi"}},
	}

	t = time.Now()
	result, err = Scroll(l, dbPath, client, index, filter)
	if err != nil {
		log.Fatalf("Error searching: %s", err)
	}

	if result == nil {
		return
	}

	if len(result.HitSet.Hits) > 0 {
		fmt.Printf("num hits: %+v\n", len(result.HitSet.Hits))
		// fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0].Details)
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		fmt.Printf("num aggs: %+v\n", len(result.Aggregations.Stats.Buckets))
		// fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	}
	fmt.Printf("took: %s\n\n", time.Since(t))

	t = time.Now()
	result, err = Search(l, client, index, bomQuery)
	if err != nil {
		log.Fatalf("Error searching: %s", err)
	}

	if len(result.HitSet.Hits) > 0 {
		fmt.Printf("num hits: %+v\n", len(result.HitSet.Hits))
		// fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0].Details)
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		fmt.Printf("num aggs: %+v\n", len(result.Aggregations.Stats.Buckets))
		// fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	}
	fmt.Printf("took: %s\n\n", time.Since(t))

	t = time.Now()
	result, err = Scroll(l, dbPath, client, index, filter)
	if err != nil {
		log.Fatalf("Error searching: %s", err)
	}

	if result == nil {
		return
	}

	if len(result.HitSet.Hits) > 0 {
		fmt.Printf("num hits: %+v\n", len(result.HitSet.Hits))
		fmt.Printf("first hit: %+v\n", result.HitSet.Hits[0].Details)
	}

	if len(result.Aggregations.Stats.Buckets) > 0 {
		fmt.Printf("num aggs: %+v\n", len(result.Aggregations.Stats.Buckets))
		// fmt.Printf("first agg: %+v\n", result.Aggregations.Stats.Buckets[0])
	}
	fmt.Printf("took: %s\n\n", time.Since(t))
}

func initDB(dbPath string, client *es.Client) error {
	lte := "2024-06-10T00:00:00Z"
	gte := "2024-05-01T00:00:00Z"
	if testPeriod == 1 {
		gte = "2024-06-09T23:50:00Z"
	} else if testPeriod == 2 {
		lte = "2024-06-04T00:00:00Z"
		gte = "2024-06-02T00:00:00Z"
	}

	filter := es.Filter{
		{"match_phrase": map[string]interface{}{"META_CLUSTER_NAME": "farm"}},
		{"range": map[string]interface{}{
			"timestamp": map[string]string{
				"lte":    lte,
				"gte":    gte,
				"format": "strict_date_optional_time",
			},
		}},
	}

	query := &es.Query{
		Size:  maxSize,
		Sort:  []string{"timestamp", "_doc"},
		Query: &es.QueryFilter{Bool: es.QFBool{Filter: filter}},
	}

	t := time.Now()

	result, err := client.Scroll(index, query)
	if err != nil {
		return err
	}

	fmt.Printf("\nsearch took: %s\n", time.Since(t))
	t = time.Now()

	err = storeInLocalDB(dbPath, result)
	if err != nil {
		return err
	}

	fmt.Printf("store took: %s\n", time.Since(t))

	return nil
}

type FlatDB struct {
	path  string
	f     *os.File
	w     *bufio.Writer
	n     int64
	index int
}

func NewFlatDB(path string) (*FlatDB, error) {
	f, w, err := createFileAndWriter(path, 0)
	if err != nil {
		return nil, err
	}

	return &FlatDB{
		path: path,
		f:    f,
		w:    w,
	}, nil
}

func createFileAndWriter(path string, index int) (*os.File, *bufio.Writer, error) {
	err := os.MkdirAll(path, dbDirPerms)
	if err != nil {
		return nil, nil, err
	}

	f, err := os.Create(fmt.Sprintf("%s/%d", path, index))
	if err != nil {
		return nil, nil, err
	}

	w := bufio.NewWriterSize(f, fileBufferSize)

	return f, w, nil
}

func (f *FlatDB) Store(fields ...[]byte) error {
	total := 0

	for _, field := range fields {
		n, err := f.w.Write(field)
		total += n

		if err != nil {
			return err
		}
	}

	f.n += int64(total)
	if f.n > desiredFlatDBSize {
		err := f.Close()
		if err != nil {
			return err
		}

		f.index++

		file, w, err := createFileAndWriter(f.path, f.index)
		if err != nil {
			return err
		}

		f.f = file
		f.w = w
		f.n = 0
	}

	return nil
}

func (f *FlatDB) Close() error {
	f.w.Flush()

	return f.f.Close()
}

type LocalDB struct {
	dir string
	dbs map[string]*FlatDB
}

func NewLocalDB(dir string) (*LocalDB, error) {
	err := os.MkdirAll(dir, dbDirPerms)
	if err != nil {
		return nil, err
	}

	return &LocalDB{
		dir: dir,
		dbs: make(map[string]*FlatDB),
	}, nil
}

func (l *LocalDB) GetFlatDB(timestamp int64, bom string) (*FlatDB, error) {
	key := fmt.Sprintf("%s/%s", time.Unix(timestamp, 0).UTC().Format(dateFormat), bom)

	fdb, ok := l.dbs[key]
	if !ok {
		var err error

		fdb, err = NewFlatDB(filepath.Join(l.dir, key))
		if err != nil {
			return nil, err
		}

		l.dbs[key] = fdb
	}

	return fdb, nil
}

func (l *LocalDB) Close() error {
	for _, fdb := range l.dbs {
		if err := fdb.Close(); err != nil {
			return err
		}
	}

	return nil
}

func storeInLocalDB(dbPath string, result *es.Result) (err error) {
	fmt.Printf("storing %d hits\n", len(result.HitSet.Hits))

	ldb, errl := NewLocalDB(dbPath)
	if errl != nil {
		return errl
	}

	defer func() {
		errc := ldb.Close()
		if err == nil {
			err = errc
		}
	}()

	bpre.Marshal(detailsBufferLength)

	for _, hit := range result.HitSet.Hits {
		group, errf := fixedWidthGroup(hit.Details.AccountingName)
		if errf != nil {
			err = errf
			return
		}

		user, errf := fixedWidthUser(hit.Details.UserName)
		if errf != nil {
			err = errf
			return
		}

		isGPU := notInGPUQueue
		if strings.HasPrefix(hit.Details.QueueName, "gpu") {
			isGPU = inGPUQueue
		}

		fdb, errl := ldb.GetFlatDB(hit.Details.Timestamp, hit.Details.BOM)
		if errl != nil {
			err = errf
			return
		}

		encoded, errs := hit.Details.Serialize()
		if errs != nil {
			err = errs
			return
		}

		errs = fdb.Store(
			i64tob(hit.Details.Timestamp),
			group,
			user,
			[]byte{isGPU},
			i32tob(int32(len(encoded))),
			encoded,
		)
		if errs != nil {
			err = errs
			return
		}
	}

	return
}

func fixedWidthGroup(group string) ([]byte, error) {
	return fixedWidthString(group, accountingNameMaxWidth, "ACCOUNTING_NAME")
}

func fixedWidthString(str string, max int, kind string) ([]byte, error) {
	padding := max - len(str)
	if padding < 0 {
		return nil, errors.New(kind + " too long")
	}

	return []byte(str + strings.Repeat(" ", padding)), nil
}

func fixedWidthUser(user string) ([]byte, error) {
	return fixedWidthString(user, userNameMaxWidth, "USER_NAME")
}

// i64tob returns an 8-byte big endian representation of v. The result is a
// sortable byte representation of something like a unix time stamp in seconds.
func i64tob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

func i32tob(v int32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, uint32(v))
	return b
}

func btoi(b []byte) int {
	return int(binary.BigEndian.Uint32(b[0:4]))
}

func Search(l *lru.Cache[string, *es.Result], client *es.Client, index string, query *es.Query) (*es.Result, error) {
	cacheKey := query.Key()

	result, ok := l.Get(cacheKey)
	if ok {
		return result, nil
	}

	result, err := client.Search(index, query)
	if err != nil {
		return nil, err
	}

	l.Add(cacheKey, result)

	return result, nil
}

func Scroll(l *lru.Cache[string, *es.Result], dbPath string, client *es.Client, index string, filter es.Filter) (*es.Result, error) {
	query := &es.Query{
		Size:  maxSize,
		Sort:  []string{"_doc"},
		Query: &es.QueryFilter{Bool: es.QFBool{Filter: filter}},
	}

	cacheKey := query.Key()

	result, ok := l.Get(cacheKey)
	if ok {
		return result, nil
	}

	result, err := searchLocal(dbPath, query)
	if err != nil {
		return nil, err
	}

	l.Add(cacheKey, result)

	return result, nil
}

type LocalFilter struct {
	BOM             string
	LTE             time.Time
	GTE             time.Time
	LTEKey          []byte
	GTEKey          []byte
	accountingName  []byte
	userName        []byte
	checkAccounting bool
	checkUser       bool
	checkGPU        bool
}

func NewLocalFilter(query *es.Query) (*LocalFilter, error) {
	lte, gte, err := query.DateRange()
	if err != nil {
		return nil, err
	}

	filter := &LocalFilter{
		LTE: lte,
		GTE: gte,
	}

	filter.LTEKey, filter.GTEKey = i64tob(lte.Unix()), i64tob(gte.Unix())

	// lteStamp := time.Unix(btoi64(filter.LTEKey), 0).UTC().Format(time.RFC3339)

	filter.BOM, filter.accountingName, filter.userName, filter.checkGPU = queryToFilters(query)

	if filter.BOM == "" {
		return nil, errors.New("BOM not specified")
	}

	filter.checkAccounting = len(filter.accountingName) > 0
	filter.checkUser = len(filter.userName) > 0

	return filter, nil
}

func (f *LocalFilter) PassesAccountingName(val []byte) bool {
	if !f.checkAccounting {
		return true
	}

	return bytes.Equal(val, f.accountingName)
}

func (f *LocalFilter) PassesUserName(val []byte) bool {
	if !f.checkUser {
		return true
	}

	return bytes.Equal(val, f.userName)
}

func (f *LocalFilter) PassesGPUCheck(val byte) bool {
	if !f.checkGPU {
		return true
	}

	return val == inGPUQueue
}

func searchLocal(dbPath string, query *es.Query) (*es.Result, error) {
	dateBOMDirs := make(map[string][]string)

	err := filepath.WalkDir(dbPath, func(path string, d fs.DirEntry, err error) error {
		if !d.Type().IsRegular() {
			return nil
		}

		dir := filepath.Dir(path)

		dateBOMDirs[dir] = append(dateBOMDirs[dir], path)

		return nil
	})
	if err != nil {
		return nil, err
	}

	filter, err := NewLocalFilter(query)
	if err != nil {
		return nil, err
	}

	errCh := make(chan error)
	result := &es.Result{
		HitSet: &es.HitSet{},
	}

	errsDoneCh := make(chan struct{})
	go func() {
		for err := range errCh {
			fmt.Printf("error: %s\n", err)
		}

		close(errsDoneCh)
	}()

	var wg sync.WaitGroup

	currentDay := filter.GTE

	for {
		fileKey := currentDay.UTC().Format(dateFormat)
		paths := dateBOMDirs[fmt.Sprintf("%s/%s/%s", dbPath, fileKey, filter.BOM)]

		for _, path := range paths {
			wg.Add(1)
			go func(dbFilePath string) {
				defer wg.Done()

				searchLocalFile(dbFilePath, filter, result.HitSet, errCh)
			}(path)
		}

		currentDay = currentDay.Add(24 * time.Hour)
		if currentDay.After(filter.LTE) {
			break
		}
	}

	wg.Wait()
	close(errCh)
	<-errsDoneCh

	return result, nil
}

func searchLocalFile(dbFilePath string, filter *LocalFilter, hitset *es.HitSet, errCh chan error) {
	f, err := os.Open(dbFilePath)
	if err != nil {
		errCh <- err

		return
	}

	tsBuf := make([]byte, timeStampLength)
	accBuf := make([]byte, accountingNameMaxWidth)
	userBuf := make([]byte, userNameMaxWidth)
	lenBuf := make([]byte, lengthEncodeLength)
	detailsBuf := make([]byte, detailsBufferLength)

	br := bufio.NewReaderSize(f, fileBufferSize)

	t := time.Now()
	total := 0
	numGoroutines := runtime.NumGoroutine()

	for {
		total++

		_, err = io.ReadFull(br, tsBuf)
		if err != nil {
			if !errors.Is(err, io.EOF) {
				errCh <- err

				return
			}

			break
		}

		if bytes.Compare(tsBuf, filter.LTEKey) > 0 {
			break
		}

		var passesFilter bool

		if bytes.Compare(tsBuf, filter.GTEKey) >= 0 {
			passesFilter = true
		}

		_, err = io.ReadFull(br, accBuf)
		if err != nil {
			errCh <- err

			return
		}

		if passesFilter && !filter.PassesAccountingName(accBuf) {
			passesFilter = false
		}

		_, err = io.ReadFull(br, userBuf)
		if err != nil {
			errCh <- err

			return
		}

		if passesFilter && !filter.PassesUserName(userBuf) {
			passesFilter = false
		}

		gpuByte, err := br.ReadByte()
		if err != nil {
			errCh <- err

			return
		}

		if passesFilter && !filter.PassesGPUCheck(gpuByte) {
			passesFilter = false
		}

		_, err = io.ReadFull(br, lenBuf)
		if err != nil {
			errCh <- err

			return
		}

		detailsLength := btoi(lenBuf)

		buf := detailsBuf[0:detailsLength]

		_, err = io.ReadFull(br, buf)
		if err != nil {
			errCh <- err

			return
		}

		if passesFilter {
			d, err := es.DeserializeDetails(buf)
			if err != nil {
				errCh <- err

				return
			}

			hitset.AddHit("", d)
		}
	}

	if debug {
		fmt.Printf("%s took %s for %d hits, starting with %d goroutines\n", dbFilePath, time.Since(t), total, numGoroutines)
	}

	f.Close()
}

func queryToFilters(query *es.Query) (bom string, accountingName, userName []byte, checkGPU bool) {
	filters := query.Filters()

	bom = filters["BOM"]

	aname, ok := filters["ACCOUNTING_NAME"]
	if ok {
		if b, err := fixedWidthGroup(aname); err == nil {
			accountingName = b
		}
	}

	uname, ok := filters["USER_NAME"]
	if ok {
		if b, err := fixedWidthUser(uname); err == nil {
			userName = b
		}
	}

	qname, ok := filters["QUEUE_NAME"]
	if ok && strings.HasPrefix(qname, "gpu") {
		checkGPU = true
	}

	return
}
