# big-file-sort

<p>
Реализовано создание больших файлов с заданной средней длиной строки. 
</br>
Main - class для создания больших файлов: ru.kolpakov.tasks.sort.GenerateBigFile
</br>
Входные параметры:
<ul>
<li>путь к файлу в который нужно будет записать строки</li>
<li>количество строк, которое нужно сгенерировать</li>
<li>средний размер строки</li>
</ul>
</p>

<p>
Реализована сортировка большого файла, не помещающегося в память
</br>
Main класс для создания больших файлов: ru.kolpakov.tasks.sort.App
</br>
Входные параметры:
<ul>
<li>путь к файлу, который нужно отсортироватьь</li>
<li>путь, по которому будет создана директория для хранения временных файлов, перед началом исполнения должна быть пустой</li>
<li>путь, по которому будет записан отсортированный файл, по этому пути не должно существовать файлов или папок</li>
</ul>
</p>

<p>
A little bit of a disclaimer:
</br>
Java не лучший язык для подобного рода алгоритмов. Имеем высокий оверхэд по памяти относительно сортируемой строки из-за внутреннего представления строк в Java: много памяти тратится на класс строки, все символы хранятся в char, что может быть невыгодно для строк с большим количеством символов из таблицы ASCII. Java не имеет в стандартной библиотеке сортировки in-place, что также увеличивает объем требуемой памяти. И в целом прямое управление памтью в этом задании может дать большой performance boost. 
</br>
Все эти препятствия можно было бы обойти, но это потребовало бы более 4, данных на задания, часов.
</p>
<p>
Решение не идеально оптимизировано для hdd(писалось и тестировалась на единственном на ноутбуке ssd): в первой стадии параллельной сортировки стоит добавить семафор с числом 1 для всех секций кода, использующих диск. Во второй стадии слияния стоит увеличить размер буффера в используемых BufferedReader для снижения количества необходимых чтений с диска.
</p>
