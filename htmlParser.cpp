#include <iostream>
#include <fstream>
#include <string>
#include <unordered_set>
#include <cctype>
#include <filesystem>
#include <vector>

using namespace std;
namespace fs = std::filesystem;

// --------- Word Filter ---------
bool isValidWord(const string &w) {
    if (w.size() <= 3 || w.size() >= 15)
        return false;

    const string vowels = "aeiou";
    bool hasVowel = false;

    for (char c : w) {
        if (vowels.find(c) != string::npos) {
            hasVowel = true;
            break;
        }
    }
    if (!hasVowel) return false;

    int cons = 0;
    for (char c : w) {
        if (vowels.find(c) == string::npos) {
            cons++;
            if (cons >= 4) return false;
        } else {
            cons = 0;
        }
    }

    return true;
}

// --------- Add all easy 1-3 letter words ---------
void easierWords(unordered_set<string> &lex) {
    vector<string> easy = {
        // 1-letter
        "a","i","o",

        // 2-letter
        "aa","ab","ad","ae","ag","ah","ai","al","am","an","ar","as","at","aw","ax","ay",
        "ba","be","bi","bo","by",
        "da","de","do",
        "ed","ef","eh","el","em","en","er","es","et","ew","ex",
        "fa","fe",
        "go",
        "ha","he","hi","ho",
        "id","if","in","is","it",
        "jo",
        "ka","ki",
        "la","li","lo",
        "ma","me","mi","mm","mu","my",
        "na","ne","no","nu",
        "od","oe","of","oh","oi","om","on","op","or","os","ow","ox","oy",
        "pa","pe","pi",
        "qi",
        "re",
        "sh","si","so",
        "ta","ti","to",
        "uh","um","un","up","us","ut",
        "we","wo",
        "xi","xu",
        "ya","ye","yo",

        // 3-letter
        "ace","act","add","ado","aft","age","ago","aid","ail","aim","air","ale","all","and","ant","any","ape","apt","arc","are","arm","art","ash","ask","asp","ass","ate","awe","axe","aye",
        "bad","bag","ban","bar","bat","bay","bed","bee","beg","bet","bib","bid","big","bin","bit","boa","bob","bog","boo","bop","bow","box","boy","bra","bud","bug","bun","bus","but","buy",
        "cab","cad","can","cap","car","cat","caw","cay","chi","cig","cob","cod","cog","con","coo","cop","cot","cow","coy","cry","cub","cud","cue","cup","cur","cut",
        "dab","dad","dam","day","den","dew","did","dig","dim","din","dip","dog","don","dot","dry","dub","dud","due","dug","dun","duo","dye",
        "ear","eat","ebb","eel","egg","ego","eke","elf","elk","ell","elm","end","eon","era","ere","err","eve","ewe",
        "fab","fad","fan","far","fat","fax","fey","fig","fin","fir","fit","fix","flu","fly","foe","fog","for","fox","fry","fun","fur",
        "gab","gag","gal","gap","gas","gay","gel","gem","get","gig","gin","got","gum","gun","gut","guy",
        "had","ham","has","hat","hay","hen","her","hey","hid","him","hip","his","hit","hog","hop","hot","how","hub","hug","hum","hun","hut",
        "ice","icy","ill","imp","ink","inn","ion","ire","irk","ish",
        "jab","jag","jam","jar","jaw","jay","jet","jib","jig","job","jog","jot","joy","jug","jut",
        "kab","keg","ken","key","kid","kin","kit",
        "lab","lad","lag","lap","law","lay","lea","led","leg","let","lid","lie","lip","lit","lob","log","lop","lot","low","lug",
        "mad","man","map","mat","maw","may","med","men","met","mid","mil","mix","mob","mod","mow","mud","mug","mum",
        "nab","nag","nap","nay","net","new","nib","nil","nip","nod","nog","nor","not","now","nub","nut",
        "oak","oar","oat","odd","ode","off","oft","ohm","oil","old","one","orb","ore","our","out","owl","own",
        "pad","pal","pan","par","pat","paw","pay","pea","peg","pen","pep","per","pet","pew","phi","pic","pie","pig","pin","pip","pit","pod","pop","pot","pro","psi","pub","pun","pup","put",
        "qua",
        "rad","rag","ram","ran","rap","rat","raw","ray","red","rep","rev","rib","rid","rig","rim","rip","rob","rod","roe","rot","row","rub","rue","rug","rum","run","rut",
        "sac","sad","sag","sap","sat","say","sea","see","set","sew","shy","sip","sir","sis","sit","six","sky","sly","sob","sod","son","sop","sot","soy","spa","spy","sub","sue","sun","sup",
        "tab","tad","tag","tan","tap","tar","tat","tea","tee","ten","the","tho","thy","tic","tie","tin","tip","toe","tog","tom","ton","too","top","tor","tot","tow","toy","try","tub","tug","tun","two",
        "ugh","uke","use",
        "van","vat","vet","vex","via","vie","vim",
        "wad","wag","war","was","wax","way","web","wed","wee","wen","wet","who","why","win","wit","woe","won","woo","wow",
        "yak","yam","yap","yaw","yea","yen","yes","yet","you",
        "zag","zap","zen","zip","zoo"
    };

    for (const string &w : easy) {
        lex.insert(w);
    }
}

// --------- HTML Parser ---------
void parseHTMLFile(const fs::path &htmlPath, unordered_set<string> &lex) {
    ifstream in(htmlPath, ios::binary);
    if (!in) {
        cerr << "Cannot open: " << htmlPath << endl;
        return;
    }

    bool inTag = false;
    string word;
    char prev = 0;
    char c;

    while (in.get(c)) {
        if (c == '<') { inTag = true; continue; }
        if (c == '>') { inTag = false; continue; }
        if (inTag) continue;

        // remove apostrophe+s -> don't break the word
        if (prev == '\'' && c == 's') { prev = 0; continue; }
        prev = c;

        if (isalpha(c)) {
            word.push_back(tolower(c));
        } else {
            if (!word.empty()) {
                if (isValidWord(word)) {
                    lex.insert(word);
                }
                word.clear();
            }
        }
    }

    if (!word.empty()) {
        if (isValidWord(word)) {
            lex.insert(word);
        }
    }
}

// --------- MAIN DRIVER ---------
int main() {
    fs::path rawFolder = "urls_data/raw";
    fs::path lexFile   = "lexicon.txt";

    unordered_set<string> lexicon;

    // Add all short valid words first
    easierWords(lexicon);

    int processed = 0;

    for (auto &entry : fs::directory_iterator(rawFolder)) {
        if (entry.path().extension() == ".html") {
            parseHTMLFile(entry.path(), lexicon);

            processed++;
            if (processed % 50 == 0)
                cout << processed << " files processed\n";
        }
    }

    ofstream out(lexFile);
    for (auto &w : lexicon) {
        out << w << "\n";
    }

    cout << "Done. Final lexicon size: " << lexicon.size() << endl;
    cout << "Saved to: " << lexFile << endl;
    return 0;
}
