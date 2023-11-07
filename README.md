# Многопоточный кеширующий прокси-сервер
Реализуйте простой кэширующий http-proxy с кэшем в оперативной памяти.

При поступлении http-запроса ваш прокси должен попытаться найти точный url 
запроса в индексе кэша. Если он будет обнаружен, возвратить содержимое 
соответствующего буфера кэша. При этом следуюет учесть, что буфер кэша 
может быть ещё не считан до конца.

Если url в индексе кэша не обнаружен, внести его в индекс кэша, запросить 
у указанного в url сервера размер страницы и попытаться разместить буфер 
необходимого размера в оперативной памяти. Затем необходимо инициировать 
чтение страницы с сервера и ее сохранение в буфере вместе с заголовками 
http. По мере того, как страница будет считываться, ее необходимо 
возвращать клиенту.

Если сервер не вернет размер страницы, необзодимо разместить буфер 
размером 100КБ и расширять его по мере необходимости блоками такого же 
размера. При завершении страницы необходимо сократить буфер до реального
размера страницы.

При невозможности разместить или расширить буфер (напр. из-за превышения 
квоты адресного пространства), необходимо удалить один или несколько 
буферов, которые дольше всего не использовались.

Реализовать задачу, создавая для каждого входящего http-соединения свой 
поток. Максимальное число потоков должно указываться в настройках сервера.

Необходимо обеспечить одновременную обработку количества запросов, 
превосходящего количество потоков в пуле; блокировка входящих соединений 
недопустима. Долгое ожидание возможно.

Сравнить скорость работы сервера при различных настройках количества
потоков на прокси сервере.
