package tfidf

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"math"
	"sync"

	"github.com/dgraph-io/badger"
)

type TFIDF struct {
	sync.Mutex
	pd persistentData

	// derived data, generated after persistent data loaded
	wm *wordMap
	dm *docMap
}

type WordTFIDF struct {
	Index int     `json:"Index"`
	Value float64 `json:"Value"`
}

type wordMap struct {
	sync.Mutex
	m *badger.DB // word
}

func (wm *wordMap) setWord(w word) {
	if wm == nil {
		return
	}
	defer wm.Unlock()
	wm.Lock()
	wm.m[w.Value] = &w
}

func (wm *wordMap) getWord(s string) *word {
	if wm == nil {
		return nil
	}
	defer wm.Unlock()
	wm.Lock()
	return wm.m[s]
}

//func newWordMap() *wordMap {
//	return &wordMap{
//		M: make(map[string]*word),
//	}
//}

type docMap struct {
	sync.Mutex
	m *badger.DB //DOC
}

func (dm *docMap) setDoc(d *Doc) {
	if dm == nil {
		return
	}
	defer dm.Unlock()
	dm.Lock()
	dm.m[d.ID] = d
}

func (dm *docMap) getDoc(id string) *Doc {
	if dm == nil {
		return nil
	}
	defer dm.Unlock()
	dm.Lock()
	return dm.m[id]
}

//func newDocMap() *docMap {
//	return &docMap{
//		M: make(map[string]*Doc),
//	}
//}

type word struct {
	Value  string
	Index  int
	DocSet *docSet
}

func (w *word) getIndex() int {
	if w == nil {
		return -1
	}
	return w.Index
}

func (w *word) addDoc(docID string) {
	if w == nil {
		return
	}
	w.DocSet.append(docID)
}

func (w *word) delDoc(docID string) {
	if w == nil {
		return
	}
	w.DocSet.del(docID)
}

func (w *word) docCount() int {
	if w == nil {
		return 0
	}
	return len(w.DocSet.M)
}

type persistentData struct {
	sync.Mutex
	updated bool

	// store in data file descriptor
	DocCount  int `json:"doc_count,omitempty"`
	WordCount int `json:"word_count,omitempty"`

	// store in data file
	Docs  []Doc    `json:"docs,omitempty"`
	Words []string `json:"words,omitempty"`
}

func (p *persistentData) appendWord(s string) int {
	if p == nil {
		return -1
	}
	defer p.Unlock()
	p.Lock()
	p.Words = append(p.Words, s)
	p.updated = true
	p.WordCount = len(p.Words)
	return len(p.Words) - 1
}

func (p *persistentData) appendDoc(doc Doc) int {
	if p == nil {
		return -1
	}
	defer p.Unlock()
	p.Lock()
	p.Docs = append(p.Docs, doc)
	p.updated = true
	p.DocCount = len(p.Docs)
	return len(p.Docs) - 1
}

type set map[string]struct{}

func (s set) set(str string) {
	if s == nil {
		return
	}
	s[str] = struct{}{}
}

func (s set) del(str string) {
	if s == nil {
		return
	}
	delete(s, str)
}

func (s set) exist(str string) bool {
	if s == nil {
		return false
	}
	_, ok := s[str]
	return ok
}

func (s set) members() []string {
	if s == nil {
		return nil
	}
	members := make([]string, 0, len(s))
	for member := range s {
		members = append(members, member)
	}
	return members
}

type docSet struct {
	sync.Mutex
	M set
}

func newSet() *docSet {
	return &docSet{
		M: make(set),
	}
}

func (s *docSet) append(str string) *docSet {
	if s == nil {
		return nil
	}
	defer s.Unlock()
	s.Lock()
	s.M.set(str)
	return s
}

func (s *docSet) del(str string) {
	if s == nil {
		return
	}
	defer s.Unlock()
	s.Lock()
	s.M.del(str)
}

type Doc struct {
	ID    string   `json:"id"`
	Words []string `json:"words"`
}

func (d *Doc) wordsDiff(newWords []string) (incr, decr []string) {
	if d == nil {
		return nil, nil
	}
	incrSet := make(set)
	decrSet := make(set)
	for i := range d.Words {
		incrSet.set(d.Words[i])
		decrSet.set(d.Words[i])
	}
	for i := range newWords {
		if !incrSet.exist(newWords[i]) {
			incr = append(incr, newWords[i])
		} else {
			decrSet.del(newWords[i])
		}
	}
	decr = decrSet.members()
	return
}

func NewTFIDF() *TFIDF {
	wm, err := badger.Open(badger.DefaultOptions("./wm"))
	if err != nil {
		log.Fatalln(err)
	}
	dm, err := badger.Open(badger.DefaultOptions("./dm"))
	if err != nil {
		log.Fatalln(err)
	}

	return &TFIDF{
		wm: &wordMap{m: wm},
		dm: &docMap{m: dm},
	}
}

func (t *TFIDF) LoadFrom(pdFilename, fdFilename string) error {
	defer t.Unlock()
	t.Lock()

	fdData, err := ioutil.ReadFile(fdFilename)
	if err != nil {
		return err
	}
	fd := persistentData{}
	err = json.Unmarshal(fdData, &fd)
	if err != nil {
		return err
	}
	t.pd.DocCount = fd.DocCount
	t.pd.WordCount = fd.WordCount

	data, err := ioutil.ReadFile(pdFilename)
	if err != nil {
		return err
	}
	pd := persistentData{
		Docs:  make([]Doc, t.pd.DocCount),
		Words: make([]string, t.pd.WordCount),
	}
	err = json.Unmarshal(data, &pd)
	if err != nil {
		return err
	}

	t.pd.Docs = make([]Doc, len(pd.Docs))
	t.pd.Words = make([]string, len(pd.Words))

	for i := range pd.Docs {
		doc := Doc{
			ID:    pd.Docs[i].ID,
			Words: make([]string, len(pd.Docs[i].Words)),
		}
		copy(doc.Words, pd.Docs[i].Words)
		t.pd.Docs[i] = doc
	}
	copy(t.pd.Words, pd.Words)

	t.initDerivedData()
	return nil
}

func (t *TFIDF) initDerivedData() {
	for i := range t.pd.Words {
		t.wm.setWord(word{
			DocSet: newSet(),
			Index:  i,
			Value:  t.pd.Words[i],
		})
	}
	for i := range t.pd.Docs {
		t.dm.setDoc(&t.pd.Docs[i])
		for j := range t.pd.Docs[i].Words {
			w := t.wm.getWord(t.pd.Docs[i].Words[j])
			if w == nil {
				t.appendWord(t.pd.Docs[i].Words[j], t.pd.Docs[i].ID)
				continue
			}
			w.addDoc(t.pd.Docs[i].ID)
			t.wm.setWord(*w)
		}
	}
}

func (t *TFIDF) appendWord(s, docID string) {
	// check if the word already exists
	w := t.wm.getWord(s)
	if w != nil {
		return
	}
	w = new(word)
	w.DocSet = newSet()
	w.DocSet.append(s)
	w.Value = s
	w.Index = t.pd.appendWord(s)
	t.wm.setWord(*w)
}

func (t *TFIDF) Save(pdFilename, fdFilename string) error {
	t.Lock()
	if !t.pd.updated {
		t.Unlock()
		return nil
	}

	pd := persistentData{
		Docs:  make([]Doc, len(t.pd.Docs)),
		Words: make([]string, len(t.pd.Words)),
	}
	copy(pd.Docs, t.pd.Docs)
	copy(pd.Words, t.pd.Words)
	t.pd.updated = false

	fd := persistentData{
		DocCount:  t.pd.DocCount,
		WordCount: t.pd.WordCount,
	}

	t.Unlock()

	data, err := json.Marshal(pd)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(pdFilename, data, 0777)
	if err != nil {
		return err
	}

	fdData, err := json.Marshal(fd)
	if err != nil {
		return err
	}

	err = ioutil.WriteFile(fdFilename, fdData, 0777)
	if err != nil {
		return err
	}

	return nil
}

func (t *TFIDF) DocCount() int {
	return len(t.pd.Docs)
}

func (t *TFIDF) WordCount() int {
	return len(t.pd.Words)
}

func (t *TFIDF) TF(doc Doc, word string) float64 {
	count := 0
	for i := range doc.Words {
		if doc.Words[i] == word {
			count++
		}
	}
	return float64(count) / float64(len(doc.Words))
}

func (t *TFIDF) TFVector(doc Doc) []float64 {
	countMap := make(map[string]int)
	for i := range doc.Words {
		countMap[doc.Words[i]]++
	}
	res := make([]float64, 0, len(doc.Words))
	for i := range doc.Words {
		res = append(res, float64(countMap[doc.Words[i]])/float64(len(doc.Words)))
	}
	return res
}

func (t *TFIDF) IDF(w string) float64 {
	return math.Log(float64(t.DocCount()) / float64(t.wm.getWord(w).docCount()+1))
}

func (t *TFIDF) IDFVector(doc Doc) []float64 {
	res := make([]float64, 0, len(doc.Words))
	for i := range doc.Words {
		res = append(res, t.IDF(doc.Words[i]))
	}
	return res
}

func (t *TFIDF) GetDocVector(doc Doc) []*WordTFIDF {
	t.UpsertDocs([]Doc{doc})

	res := make([]*WordTFIDF, 0, len(doc.Words))
	values := t.dotProduct(t.TFVector(doc), t.IDFVector(doc))
	for i := range doc.Words {
		res = append(res, &WordTFIDF{
			Index: t.wm.getWord(doc.Words[i]).getIndex(),
			Value: values[i],
		})
	}

	return res
}

func (t *TFIDF) dotProduct(a, b []float64) []float64 {
	var dp = func(x, y []float64) []float64 {
		res := make([]float64, len(x))
		for i := range y {
			res[i] = x[i] * y[i]
		}
		return res
	}
	if len(a) > len(b) {
		return dp(a, b)
	}
	return dp(b, a)
}

// documents shares the same id would be saved by `Last Write Wins` strategy
func (t *TFIDF) UpsertDocs(docs []Doc) {
	for i := range docs {
		t.upsertDoc(docs[i])
	}
}

func (t *TFIDF) upsertDoc(doc Doc) {
	defer t.Unlock()
	t.Lock()

	preDoc := t.dm.getDoc(doc.ID)
	if preDoc == nil {
		i := t.pd.appendDoc(doc)
		t.dm.setDoc(&t.pd.Docs[i])
		t.reIndexWords(doc)
		return
	}

	incr, decr := preDoc.wordsDiff(doc.Words)
	if len(incr) > 0 {
		t.reIndexWords(doc)
	}
	for i := range decr {
		w := t.wm.getWord(decr[i])
		if w == nil {
			continue
		}
		w.delDoc(doc.ID)
	}

	t.pd.Lock()
	preDoc.Words = doc.Words
	t.pd.updated = true
	t.pd.Unlock()
}

func (t *TFIDF) reIndexWords(doc Doc) {
	for i := range doc.Words {
		w := t.wm.getWord(doc.Words[i])
		if w == nil {
			t.wm.setWord(word{
				Value:  doc.Words[i],
				Index:  t.pd.appendWord(doc.Words[i]),
				DocSet: newSet().append(doc.ID),
			})
			continue
		}
		w.DocSet.append(doc.ID)
	}
}
